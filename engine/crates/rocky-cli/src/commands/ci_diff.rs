//! `rocky ci-diff` — detect changed models between git refs and generate a structural diff report.
//!
//! Shells out to `git diff --name-status` to find `.sql`, `.rocky`, and `.toml`
//! sidecar files that changed between a base ref (default: `main`) and HEAD.
//! Compiles the current working tree to extract model schemas, then classifies
//! each changed model as added, modified, or removed and generates a structured
//! diff report in JSON and Markdown formats.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use tracing::debug;

use rocky_compiler::compile::{self, CompilerConfig};
use rocky_core::ci_diff::{
    ColumnChangeType, ColumnDiff, DiffResult, DiffSummary, ModelDiffStatus, format_diff_markdown,
    format_diff_table,
};
use rocky_core::models::Model;
use rocky_sql::defer::CollisionIdentity;

use crate::output::{CiDiffOutput, print_json};

// ---------------------------------------------------------------------------
// Git integration
// ---------------------------------------------------------------------------

/// A file change detected by git between two refs.
#[derive(Debug, Clone)]
struct ChangedFile {
    /// Path relative to the repository root.
    path: String,
    /// Previous path for a Git rename or copy record.
    old_path: Option<String>,
    /// Git diff status: A (added), D (deleted), M (modified), R (renamed), etc.
    status: char,
}

/// A changed model keyed internally by its resolved target when both refs
/// compile, or by filename stem as a best-effort fallback.
#[derive(Debug, Default)]
struct ModelChange {
    /// Internal model name shown in the structural report.
    model_name: String,
    /// Schema-map key on the base ref.
    base_schema_name: Option<String>,
    /// Schema-map key on HEAD.
    head_schema_name: Option<String>,
    /// Resolved `catalog.schema.table` this change refers to, when both refs
    /// compiled. The map key can carry a name qualifier to disambiguate two
    /// models sharing a target; this is the bare warehouse identity, which is
    /// what the report publishes.
    resolved_target: Option<String>,
    /// The physical object this row is about, whether or not it is
    /// publishable.
    ///
    /// Distinct from `resolved_target`, which is withheld when a component
    /// could never name a warehouse object. Occupancy still has to be decided
    /// for those rows, so it is tracked separately rather than inferred by
    /// un-qualifying the map key — a target that failed validation may itself
    /// contain the `#` the key uses as its qualifier separator.
    target: Option<CollisionIdentity>,
    /// Some model in the base project resolved to `target`, whether or not it
    /// is the model this row is keyed on.
    base_target_occupied: bool,
    /// Likewise for head.
    head_target_occupied: bool,
}

impl ModelChange {
    /// A row identifies a **warehouse target**, so Added and Removed are
    /// statements about that target rather than about one model's assignment.
    ///
    /// Those coincide except when two models share a target: they get
    /// name-qualified keys so neither vanishes from the report, and the
    /// qualification then makes the judgement per-*model*. Retarget one of a
    /// colliding pair and its old target was reported `removed` although the
    /// other model still writes it — nothing was abandoned, and a reader
    /// acting on that would drop a live table (#1236).
    ///
    /// Occupancy is therefore consulted alongside authorship: a target no
    /// head model resolves to is Removed; one no base model resolved to is
    /// Added; a target present on both sides is Modified however its
    /// producers moved around.
    fn status(&self) -> ModelDiffStatus {
        match (
            self.base_schema_name.is_some() || self.base_target_occupied,
            self.head_schema_name.is_some() || self.head_target_occupied,
        ) {
            (true, true) => ModelDiffStatus::Modified,
            (true, false) => ModelDiffStatus::Removed,
            (false, true) => ModelDiffStatus::Added,
            (false, false) => ModelDiffStatus::Unchanged,
        }
    }
}

/// Reject a `base_ref` that git would interpret as an option rather than a
/// revision.
///
/// `base_ref` is operator- or CI-supplied (commonly wired from a PR/branch
/// variable such as `${{ github.base_ref }}`). The diff helpers below pass it
/// to `git diff` / `git ls-tree` / `git show` as a positional argument, so a
/// value beginning with `-` could smuggle flags into those invocations
/// (argument injection). Refs that begin with `-` are never valid revisions,
/// and an empty ref is meaningless here, so rejecting both fully closes the
/// vector without constraining legitimate refs (`origin/main`, `HEAD~3`, a SHA).
pub(crate) fn validate_base_ref(base_ref: &str) -> Result<()> {
    if base_ref.is_empty() {
        anyhow::bail!("base ref must not be empty");
    }
    if base_ref.starts_with('-') {
        anyhow::bail!(
            "invalid base ref '{base_ref}': must not begin with '-' (git would read it as an option)"
        );
    }
    Ok(())
}

/// Build a `git` invocation, optionally rooted at an explicit repository
/// directory instead of the process's current one.
///
/// Production always passes `None` and inherits the process cwd, which is the
/// long-standing behavior. Tests pass `Some(dir)` so they can drive a scratch
/// repository without calling `set_current_dir` — cwd is process-wide, and this
/// crate's tests run as threads in one process under `cargo test`, so mutating
/// it races every other test that reads it (directly or through the many
/// commands that resolve relative paths).
fn git_in(repo_dir: Option<&Path>) -> Command {
    let mut cmd = Command::new("git");
    if let Some(dir) = repo_dir {
        cmd.current_dir(dir);
    }
    cmd
}

/// Run `git diff --name-status` between `base_ref` and HEAD to find changed files.
///
/// Uses three-dot syntax (`base...HEAD`) for merge-base semantics — this matches
/// what CI systems care about: changes since the branch diverged from the base,
/// not changes since the base's current tip.
fn git_changed_files(base_ref: &str, repo_dir: Option<&Path>) -> Result<Vec<ChangedFile>> {
    let output = git_in(repo_dir)
        .args(["diff", "--name-status", &format!("{base_ref}...HEAD")])
        .output()
        .context("failed to run `git diff` — is git installed and is this a git repository?")?;

    if !output.status.success() {
        // Fall back to two-dot syntax if three-dot fails (e.g. shallow clone
        // without the base ref). This is less precise but better than failing.
        debug!(
            "three-dot git diff failed (exit {}), falling back to two-dot",
            output.status
        );
        let output = git_in(repo_dir)
            .args(["diff", "--name-status", base_ref, "HEAD"])
            .output()
            .context("failed to run `git diff` with two-dot syntax")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("git diff failed: {stderr}");
        }

        return parse_name_status(&output.stdout);
    }

    parse_name_status(&output.stdout)
}

/// Parse the output of `git diff --name-status`.
fn parse_name_status(raw: &[u8]) -> Result<Vec<ChangedFile>> {
    let text = String::from_utf8_lossy(raw);
    let mut files = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Format: "<status>\t<path>" (or "<status>\t<old>\t<new>" for renames)
        let mut parts = line.splitn(3, '\t');
        let status_str = parts.next().unwrap_or("");
        let first_path = parts.next().unwrap_or("");
        let status = status_str.chars().next().unwrap_or('M');
        let second_path = parts.next();
        let (path, old_path) = match (status, second_path) {
            ('R' | 'C', Some(new_path)) => (new_path, Some(first_path.to_string())),
            _ => (first_path, None),
        };

        if !path.is_empty() {
            files.push(ChangedFile {
                path: path.to_string(),
                old_path,
                status,
            });
        }
    }

    Ok(files)
}

/// Return the file name of `path` when it sits directly inside the models root.
///
/// Model loading is flat, so anything nested — `groups/*.toml`, for instance —
/// is not a model file and yields `None`.
fn model_file_name<'a>(path: &'a str, models_rel: Option<&str>) -> Option<&'a str> {
    let relative = match models_rel {
        Some(root) => Path::new(path).strip_prefix(root).ok()?,
        None => Path::new(path),
    };
    if models_rel.is_some() && relative.components().count() != 1 {
        return None;
    }
    relative.file_name()?.to_str()
}

/// Whether `path` is a file the compiler could read, anywhere beneath the
/// models root.
///
/// Broader than [`model_file_name`] on purpose: it also matches the shared
/// config that feeds the compiler without being a model file itself
/// (`_defaults.toml`, `groups/*.toml`, `test_definitions.toml`). Narrower than
/// "any descendant", though — a `models/README.md` or a stray CSV cannot change
/// a compile, and running two compiles for one is pure cost. Used to decide
/// whether a compile is worth running, never to classify a model.
/// Whether `path` is shared model configuration — config the compiler reads for
/// *many* models rather than one.
///
/// These files own no model, so the path-matching classifier can never key on
/// them, yet `groups/*.toml` supplies `schema_template` and `_defaults.toml`
/// supplies `target.catalog` / `target.schema` — either can move every model
/// that reads it.
fn is_shared_model_config(path: &str, models_rel: Option<&str>) -> bool {
    let Some(root) = models_rel else {
        return false;
    };
    let Ok(relative) = Path::new(path).strip_prefix(root) else {
        return false;
    };
    if relative.extension().and_then(|ext| ext.to_str()) != Some("toml") {
        return false;
    }
    let components: Vec<_> = relative.iter().filter_map(|c| c.to_str()).collect();
    match components.as_slice() {
        [file] => matches!(*file, "_defaults.toml" | "test_definitions.toml"),
        ["groups", _] => true,
        _ => false,
    }
}

fn compiler_input_under_models_root(path: &str, models_rel: Option<&str>) -> bool {
    models_rel.is_some_and(|root| Path::new(path).strip_prefix(root).is_ok())
        && matches!(
            Path::new(path).extension().and_then(|ext| ext.to_str()),
            Some("sql" | "rocky" | "toml")
        )
}

/// Return the model stem for a changed path under the configured models root.
///
/// Filename inference, used only on the fallback path where no compile is
/// available to resolve ownership properly. Contract sidecars use
/// `<stem>.contract.toml`.
fn model_stem(path: &str, models_rel: Option<&str>) -> Option<String> {
    let file_name = model_file_name(path, models_rel)?;
    let relative = Path::new(file_name);
    let is_toml = relative.extension().and_then(|ext| ext.to_str()) == Some("toml");
    let stem = if let Some(stem) = file_name.strip_suffix(".contract.toml") {
        stem
    } else {
        match relative.extension().and_then(|ext| ext.to_str()) {
            Some("sql" | "rocky" | "toml") => relative.file_stem()?.to_str()?,
            _ => return None,
        }
    };

    // `_defaults` / `rocky` / `Cargo` name shared config files, not models — but
    // only as TOML. `models/rocky.sql` is a perfectly ordinary model and the
    // loaders accept it, so the exclusion must not reach other extensions.
    let reserved = is_toml && matches!(stem, "_defaults" | "rocky" | "Cargo");
    (!stem.is_empty() && !reserved).then(|| stem.to_string())
}

/// Whether `model` owns the file named `file_name`: its own source file, its
/// `<stem>.toml` sidecar, or its `<stem>.contract.toml` contract.
///
/// Ownership is derived from the model's own `file_path` rather than inferred
/// from the changed filename. Inference cannot tell `orders.contract.toml`
/// (the sidecar of `orders.contract.sql`) from `orders.contract.toml` (the
/// contract of `orders.sql`), and it wrongly excludes models whose stem is
/// `rocky`, `Cargo`, or `_defaults`.
fn model_owns_file(model: &Model, file_name: &str) -> bool {
    let source = Path::new(&model.file_path);
    if source.file_name().and_then(|name| name.to_str()) == Some(file_name) {
        return true;
    }
    let Some(stem) = source.file_stem().and_then(|value| value.to_str()) else {
        return false;
    };
    file_name
        .strip_prefix(stem)
        .is_some_and(|rest| matches!(rest, ".toml" | ".contract.toml"))
}

fn model_matches_path(model: &Model, path: &str, models_rel: Option<&str>) -> bool {
    model_file_name(path, models_rel).is_some_and(|name| model_owns_file(model, name))
}

/// Target identities that more than one model in `models` resolves to.
///
/// `Project::from_models` rejects duplicate `config.name` but says nothing
/// about duplicate targets, so keying the change map purely by target would let
/// one model silently overwrite another and vanish from the report. Colliding
/// targets get name-qualified keys instead.
fn ambiguous_targets(models: &[Model]) -> HashSet<CollisionIdentity> {
    let mut seen = HashSet::new();
    let mut duplicated = HashSet::new();
    for model in models {
        let target = collision_identity(model);
        if !seen.insert(target.clone()) {
            duplicated.insert(target);
        }
    }
    duplicated
}

/// The target as the user spelled it. Published, never compared.
fn target_identity(model: &Model) -> String {
    let target = &model.config.target;
    format!("{}.{}.{}", target.catalog, target.schema, target.table)
}

/// The physical object the target names. Compared, never published.
///
/// One definition of "same warehouse object" for the whole command, shared
/// with the compiler's E036 and `branch promote`. Spelling is not identity:
/// `c.S.Orders` and `c.s.orders` may be one table, and a report that keyed
/// rows on the raw string emitted **two rows for one object** — one pairing
/// whichever producers happened to share a spelling on each side, so a base
/// column supplied under one spelling read as newly added under the other.
///
/// Publication still uses [`target_identity`] via `publishable_target`, so
/// the row shows the spelling the project actually uses; only pairing is
/// canonical.
fn collision_identity(model: &Model) -> CollisionIdentity {
    let target = &model.config.target;
    CollisionIdentity::of(&target.catalog, &target.schema, &target.table)
}

/// The model's target identity, but only when every component is a real SQL
/// identifier.
///
/// Target components are not validated at load — that happens later, at SQL
/// generation, where `format_table_ref` rejects them. `resolved_target` is
/// published as *the warehouse object this row describes*, so a string that
/// could never name one must not go out under that contract; such a model
/// reports `None` and is simply unidentified rather than misidentified.
///
/// This also keeps the disambiguating `{target}#{name}` map key injective:
/// a validated identifier cannot contain `#`, so a qualified key can never
/// collide with some other model's bare target.
fn publishable_target(model: &Model) -> Option<String> {
    let target = &model.config.target;
    [&target.catalog, &target.schema, &target.table]
        .iter()
        .all(|component| rocky_sql::validation::validate_identifier(component).is_ok())
        .then(|| target_identity(model))
}

fn record_model_side(
    changes: &mut HashMap<String, ModelChange>,
    key: String,
    model_name: &str,
    resolved_target: Option<&str>,
    is_base: bool,
) {
    let change = changes.entry(key).or_default();
    if change.resolved_target.is_none() {
        change.resolved_target = resolved_target.map(str::to_string);
    }
    if is_base {
        change.base_schema_name = Some(model_name.to_string());
        if change.model_name.is_empty() {
            change.model_name = model_name.to_string();
        }
    } else {
        change.head_schema_name = Some(model_name.to_string());
        change.model_name = model_name.to_string();
    }
}

/// Record one compiled model under its target-derived key.
fn record_resolved_model(
    changes: &mut HashMap<String, ModelChange>,
    model: &Model,
    ambiguous: &HashSet<CollisionIdentity>,
    is_base: bool,
) {
    let identity = collision_identity(model);
    // A target shared by two models can't identify either of them; fall
    // back to pairing by name so neither disappears from the report.
    let key = if ambiguous.contains(&identity) {
        format!("{identity}#{}", model.config.name)
    } else {
        identity.to_string()
    };
    record_model_side(
        changes,
        key.clone(),
        &model.config.name,
        publishable_target(model).as_deref(),
        is_base,
    );
    if let Some(change) = changes.get_mut(&key) {
        change.target = Some(identity);
    }
}

fn record_resolved_side(
    changes: &mut HashMap<String, ModelChange>,
    models: &[Model],
    path: &str,
    models_rel: Option<&str>,
    ambiguous: &HashSet<CollisionIdentity>,
    is_base: bool,
) {
    for model in models
        .iter()
        .filter(|model| model_matches_path(model, path, models_rel))
    {
        record_resolved_model(changes, model, ambiguous, is_base);
    }
}

/// Classify changed model files by their externally visible target identity.
///
/// Resolved pairing is only safe when both refs compiled. Otherwise the
/// classifier preserves the previous filename-based, conservative behavior.
fn classify_model_changes(
    files: &[ChangedFile],
    models_rel: Option<&str>,
    base_models: Option<&[Model]>,
    head_models: Option<&[Model]>,
) -> HashMap<String, ModelChange> {
    let mut changes = HashMap::new();

    if let (Some(base), Some(head)) = (base_models, head_models) {
        let mut ambiguous = ambiguous_targets(base);
        ambiguous.extend(ambiguous_targets(head));
        for file in files {
            if file.status == 'R'
                && let Some(old_path) = file.old_path.as_deref()
            {
                record_resolved_side(&mut changes, base, old_path, models_rel, &ambiguous, true);
                record_resolved_side(&mut changes, head, old_path, models_rel, &ambiguous, false);
            }
            record_resolved_side(&mut changes, base, &file.path, models_rel, &ambiguous, true);
            record_resolved_side(
                &mut changes,
                head,
                &file.path,
                models_rel,
                &ambiguous,
                false,
            );
        }

        // Shared config — `_defaults.toml`, `groups/*.toml`,
        // `test_definitions.toml` — owns no model file, so nothing above can
        // key on it. It can still retarget every model that reads it, and a
        // row here identifies a *target*: N tables abandoned, N created. Pair
        // the two compiles by logical name and record any pair whose resolved
        // target moved. Models the shared config did not move contribute
        // nothing, so this stays proportional to the retargeted set rather
        // than emitting a row per model in the project.
        if files
            .iter()
            .any(|file| is_shared_model_config(&file.path, models_rel))
        {
            let head_by_name: HashMap<&str, &Model> = head
                .iter()
                .map(|model| (model.config.name.as_str(), model))
                .collect();
            for base_model in base {
                let Some(head_model) = head_by_name.get(base_model.config.name.as_str()) else {
                    continue;
                };
                // Deliberately the raw spelling, not `collision_identity`.
                // This decides whether to *record*, not what a row is: a
                // case-only change is treated as a move and both sides get
                // recorded, and `record_resolved_model` then keys them
                // canonically, so they land on one row comparing the model
                // against itself. Folding here would instead skip the pair
                // and report nothing at all — the under-reporting direction,
                // on a question Rocky cannot settle without knowing the
                // warehouse's case sensitivity (#1281).
                if target_identity(base_model) == target_identity(head_model) {
                    continue;
                }
                record_resolved_model(&mut changes, base_model, &ambiguous, true);
                record_resolved_model(&mut changes, head_model, &ambiguous, false);
            }
        }

        // Occupancy pass. Everything above keys rows by (possibly name-qualified)
        // target and records which side *authored* them. Whether a target is
        // occupied is a property of the whole project, not of the changed
        // files, so it is settled here against the full model sets (#1236).
        //
        // Identity is `CollisionIdentity`, the same key the compiler's E036
        // and `branch promote` use, not the row's raw spelling. Two spellings
        // that differ only by case may name one warehouse object, and if
        // Rocky's own duplicate-target check calls them one, this must too —
        // otherwise a surviving occupant spelled `c.s.orders` fails to cover a
        // vacated `C.S.Orders` and the row still claims a removal.
        //
        // `Ephemeral` models are excluded for the reason `collide_on_target`
        // excludes them: their target is populated but phantom and nothing is
        // ever materialized there, so an ephemeral model cannot keep a table
        // alive. Counting one would turn a genuine abandonment into a
        // `Modified` row — a fail-open, and the direction that matters here,
        // since the whole point is not to mis-describe what happens to a
        // physical table.
        //
        // No schema name is borrowed from the occupant. It is tempting: the
        // row would then carry a real `column_changes` diff instead of an
        // empty one. But the row keeps its own `model_name`, and `lineage-diff`
        // traces columns through that name — so borrowing publishes another
        // model's columns under this model's identity and can name the wrong
        // downstream consumers. It is also not well founded: a base target
        // written by two colliding models had no uniquely defined schema to
        // diff against. An empty diff says what is actually known — the target
        // survives, its column delta is not attributable.
        let occupants = |models: &[Model]| {
            let mut by_target: HashSet<CollisionIdentity> = HashSet::new();
            for model in models {
                if matches!(
                    model.config.strategy,
                    rocky_core::models::StrategyConfig::Ephemeral
                ) {
                    continue;
                }
                by_target.insert(collision_identity(model));
            }
            by_target
        };
        let base_occupants = occupants(base);
        let head_occupants = occupants(head);

        for change in changes.values_mut() {
            let Some(identity) = change.target.as_ref() else {
                continue;
            };
            if change.base_schema_name.is_none() && base_occupants.contains(identity) {
                change.base_target_occupied = true;
            }
            if change.head_schema_name.is_none() && head_occupants.contains(identity) {
                change.head_target_occupied = true;
            }
        }

        return changes;
    }

    // Exactly one side compiled (or neither). Status classification below stays
    // as-is — it deliberately does not trust a single compile to decide Added
    // vs Removed. But the side that DID compile knows its own models' targets,
    // and dropping that was pure lossiness: a PR adding the first model to a
    // project has a HEAD compile that knows the target, and one deleting the
    // last model has a base compile that does (#1237).
    //
    // Only the side consistent with the row's status may supply it. An `Added`
    // row's model exists in HEAD, a `Removed` row's in base; taking a target
    // from the other side would name an object this row does not describe.
    let single_side = match (base_models, head_models) {
        (Some(base), None) => Some((base, false)),
        (None, Some(head)) => Some((head, true)),
        _ => None,
    };
    let resolve_target = |path: &str, status: ModelDiffStatus| -> Option<String> {
        let (models, is_head) = single_side?;
        let usable = match status {
            ModelDiffStatus::Added => is_head,
            ModelDiffStatus::Removed => !is_head,
            // A modification exists on both sides, so whichever side compiled
            // describes the same object.
            ModelDiffStatus::Modified => true,
            ModelDiffStatus::Unchanged => false,
        };
        if !usable {
            return None;
        }
        // Ownership of a path is NOT unique. `orders.contract.toml` is the
        // ordinary sidecar of `orders.contract.sql` *and* the contract of
        // `orders.sql`, and `model_owns_file` deliberately matches both —
        // see `ownership_resolves_contract_stem_and_contract_sidecar`. Taking
        // the first match would publish an arbitrary one of their targets
        // under a row that names the other, which breaks the field's contract
        // that it IS the warehouse object this row describes.
        //
        // So resolve every owner and publish only when they agree. Two owners
        // that happen to share a target still describe that one object, so
        // uniqueness is on the TARGET, not the model count.
        let owners = models
            .iter()
            .filter(|model| model_matches_path(model, path, models_rel));

        // Every owner must be identifiable AND they must agree. Filtering the
        // unpublishable ones out FIRST would be wrong: with two owners where
        // one has a non-identifier target, dropping it leaves exactly one
        // survivor and the path looks unique when ownership never was. The
        // ambiguity is about which model the row describes, not about whether
        // one of them happens to be nameable.
        //
        // Uniqueness is on the TARGET, not the owner count — two owners that
        // agree still describe one object. (#1291 reports two models sharing a
        // physical target as an E036 compile error, so agreement here is
        // normally the contract-sidecar overlap rather than a real collision.)
        let mut targets = Vec::new();
        for owner in owners {
            // Same publishability contract as the both-compiled path: a target
            // that could never name a warehouse object is withheld, not
            // guessed — and here it withholds the whole row, not just itself.
            targets.push(publishable_target(owner)?);
        }
        targets.sort();
        targets.dedup();
        match targets.as_slice() {
            [single] => Some(single.clone()),
            // Zero owners, or an ambiguous path: unidentified beats
            // misidentified.
            _ => None,
        }
    };

    let mut statuses = HashMap::new();
    let mut paths: HashMap<String, String> = HashMap::new();
    for file in files {
        // Fall back to the old path only for a rename, where the model really
        // did move. A copy's source is untouched, so attributing the copy to
        // that stem would report an existing model as newly Added.
        let Some(stem) = model_stem(&file.path, models_rel).or_else(|| {
            file.old_path
                .as_deref()
                .filter(|_| file.status == 'R')
                .and_then(|path| model_stem(path, models_rel))
        }) else {
            continue;
        };
        let status = match file.status {
            'A' | 'C' => ModelDiffStatus::Added,
            'D' => ModelDiffStatus::Removed,
            // Keep the prior conservative result when either ref could not
            // compile: a rename is one modification at its new path.
            _ => ModelDiffStatus::Modified,
        };
        // Remember a path per stem so the surviving compile can be asked which
        // model owns it. A rename resolves against its NEW path, matching the
        // stem the status was keyed on above.
        paths
            .entry(stem.clone())
            .or_insert_with(|| file.path.clone());
        statuses
            .entry(stem)
            .and_modify(|existing| {
                if *existing == ModelDiffStatus::Modified {
                    *existing = status;
                }
            })
            .or_insert(status);
    }

    for (name, status) in statuses {
        let (base_schema_name, head_schema_name) = match status {
            ModelDiffStatus::Added => (None, Some(name.clone())),
            ModelDiffStatus::Removed => (Some(name.clone()), None),
            ModelDiffStatus::Modified => (Some(name.clone()), Some(name.clone())),
            ModelDiffStatus::Unchanged => (None, None),
        };
        let resolved_target = paths
            .get(&name)
            .and_then(|path| resolve_target(path, status));
        changes.insert(
            name.clone(),
            ModelChange {
                model_name: name,
                base_schema_name,
                head_schema_name,
                // The key is a stem, never a warehouse identity — publishing
                // it as one would be a lie. A target only goes out when the
                // compile that DID succeed resolved it for a model owning this
                // file, and only from the side the row's status describes.
                resolved_target,
                // This is the fallback path, taken when at least one ref did
                // not compile. Occupancy is a question about a whole project,
                // so it cannot be answered from one side; leaving it unset
                // keeps `status()` exactly as conservative as it was here.
                target: None,
                base_target_occupied: false,
                head_target_occupied: false,
            },
        );
    }

    changes
}

// ---------------------------------------------------------------------------
// Schema extraction (current working tree)
// ---------------------------------------------------------------------------

/// Typed column from the compiler's type-check output.
#[derive(Debug, Clone)]
pub(crate) struct TypedColumn {
    pub(crate) name: String,
    pub(crate) data_type: String,
}

/// Project the compiler's `typed_models` map into the local `TypedColumn` shape
/// used by [`diff_columns`].
fn typed_columns_from_compile(
    result: &rocky_compiler::compile::CompileResult,
) -> HashMap<String, Vec<TypedColumn>> {
    let mut schemas = HashMap::new();
    for (model_name, typed_cols) in &result.type_check.typed_models {
        let cols: Vec<TypedColumn> = typed_cols
            .iter()
            .map(|tc| TypedColumn {
                name: tc.name.clone(),
                data_type: format!("{:?}", tc.data_type),
            })
            .collect();
        schemas.insert(model_name.clone(), cols);
    }
    schemas
}

/// Compile the models directory and return the full compile result.
///
/// `lineage-diff` needs the result's `semantic_graph` to compute downstream
/// traces; `ci-diff` only needs the per-model column schemas, which are
/// projected via [`typed_columns_from_compile`].
fn compile_head(
    models_dir: &Path,
    source_schemas: HashMap<String, Vec<rocky_compiler::types::TypedColumn>>,
) -> Result<rocky_compiler::compile::CompileResult> {
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas,
        source_column_info: HashMap::new(),
        ..Default::default()
    };

    compile::compile(&config).context("failed to compile models in the current working tree")
}

/// Try to compile the project as it stood at `base_ref` by checking out the
/// models directory at that ref into a temp directory and running the same
/// compile path as HEAD.
///
/// Returns:
/// - `Ok(result)` when the base ref's models compiled cleanly.
/// - `Err(reason)` with a short human-readable reason when the base could
///   not be materialized or did not compile. The reason is intended for the
///   semantic-diff gate's `BreakingChangesGateSkipped` audit event;
///   `compute_ci_diff` calls `.ok()` and treats `None` the same way the
///   pre-#510 `compile_base_ref` did.
///
/// `source_schemas` seeds the compile from the *current* warehouse cache,
/// not historical types. That's fine for diff purposes — typecheck on
/// historical models with today's leaf types still detects the model-level
/// schema drift that ci-diff is looking for, and there's no per-ref cache
/// to restore from.
pub fn extract_base_compile(
    base_ref: &str,
    models_dir: &Path,
    source_schemas: HashMap<String, Vec<rocky_compiler::types::TypedColumn>>,
) -> Result<rocky_compiler::compile::CompileResult, String> {
    extract_base_compile_in(base_ref, models_dir, source_schemas, None)
}

/// [`extract_base_compile`], with the git invocations optionally rooted at an
/// explicit repository directory. See [`git_in`].
fn extract_base_compile_in(
    base_ref: &str,
    models_dir: &Path,
    source_schemas: HashMap<String, Vec<rocky_compiler::types::TypedColumn>>,
    repo_dir: Option<&Path>,
) -> Result<rocky_compiler::compile::CompileResult, String> {
    let models_rel = match find_models_relative_path(models_dir, repo_dir) {
        Some(p) => p,
        None => {
            return Err("could not determine models directory relative to repo root".to_string());
        }
    };

    let tmp = match tempfile::tempdir() {
        Ok(t) => t,
        Err(e) => {
            return Err(format!(
                "failed to create temp dir for base extraction: {e}"
            ));
        }
    };

    let ls_output = git_in(repo_dir)
        .args(["ls-tree", "-r", "--name-only", base_ref, &models_rel])
        .output();
    let ls_output = match ls_output {
        Ok(o) if o.status.success() => o,
        _ => {
            return Err(format!(
                "git ls-tree failed for base ref '{base_ref}' — models directory missing at that ref?"
            ));
        }
    };

    let file_list = String::from_utf8_lossy(&ls_output.stdout);
    let mut wrote_any = false;
    for file_path in file_list.lines() {
        let file_path = file_path.trim();
        if file_path.is_empty() {
            continue;
        }
        let rel = match file_path.strip_prefix(&models_rel) {
            Some(r) => r.trim_start_matches('/'),
            None => continue,
        };
        let dest = tmp.path().join(rel);
        if let Some(parent) = dest.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let show_output = git_in(repo_dir)
            .args(["show", &format!("{base_ref}:{file_path}")])
            .output();
        if let Ok(o) = show_output
            && o.status.success()
            && std::fs::write(&dest, &o.stdout).is_ok()
        {
            wrote_any = true;
        }
    }
    if !wrote_any {
        return Err(format!("no model files found at base ref '{base_ref}'"));
    }

    let config = CompilerConfig {
        models_dir: tmp.path().to_path_buf(),
        contracts_dir: None,
        source_schemas,
        source_column_info: HashMap::new(),
        ..Default::default()
    };

    compile::compile(&config).map_err(|e| format!("base ref '{base_ref}' did not compile: {e}"))
}

/// Find the models directory path relative to the git repo root.
fn find_models_relative_path(models_dir: &Path, repo_dir: Option<&Path>) -> Option<String> {
    let abs_models = std::fs::canonicalize(models_dir).ok()?;

    let output = git_in(repo_dir)
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let repo_root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let repo_root = PathBuf::from(&repo_root);
    let repo_root = std::fs::canonicalize(&repo_root).ok()?;

    abs_models
        .strip_prefix(&repo_root)
        .ok()
        .map(|p| p.to_string_lossy().into_owned())
}

// ---------------------------------------------------------------------------
// Diff generation
// ---------------------------------------------------------------------------

/// Compare column schemas between base and head to produce column-level diffs.
fn diff_columns(base_cols: &[TypedColumn], head_cols: &[TypedColumn]) -> Vec<ColumnDiff> {
    let base_map: HashMap<&str, &str> = base_cols
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str()))
        .collect();
    let head_map: HashMap<&str, &str> = head_cols
        .iter()
        .map(|c| (c.name.as_str(), c.data_type.as_str()))
        .collect();

    let mut diffs = Vec::new();

    // Check for added or type-changed columns
    for col in head_cols {
        match base_map.get(col.name.as_str()) {
            None => {
                diffs.push(ColumnDiff {
                    column_name: col.name.clone(),
                    change_type: ColumnChangeType::Added,
                    old_type: None,
                    new_type: Some(col.data_type.clone()),
                });
            }
            Some(old_type) if *old_type != col.data_type.as_str() => {
                diffs.push(ColumnDiff {
                    column_name: col.name.clone(),
                    change_type: ColumnChangeType::TypeChanged,
                    old_type: Some(old_type.to_string()),
                    new_type: Some(col.data_type.clone()),
                });
            }
            _ => {}
        }
    }

    // Check for removed columns
    for col in base_cols {
        if !head_map.contains_key(col.name.as_str()) {
            diffs.push(ColumnDiff {
                column_name: col.name.clone(),
                change_type: ColumnChangeType::Removed,
                old_type: Some(col.data_type.clone()),
                new_type: None,
            });
        }
    }

    diffs
}

/// Build the full diff report from git changes and compiled schemas.
fn build_diff_results(
    model_changes: &HashMap<String, ModelChange>,
    head_schemas: &HashMap<String, Vec<TypedColumn>>,
    base_schemas: &HashMap<String, Vec<TypedColumn>>,
) -> Vec<DiffResult> {
    let mut results: Vec<(&str, DiffResult)> = model_changes
        .iter()
        .map(|(key, change)| {
            let status = change.status();
            let column_changes = match status {
                ModelDiffStatus::Modified => {
                    let base_cols = change
                        .base_schema_name
                        .as_ref()
                        .and_then(|name| base_schemas.get(name));
                    let head_cols = change
                        .head_schema_name
                        .as_ref()
                        .and_then(|name| head_schemas.get(name));
                    match (base_cols, head_cols) {
                        (Some(base), Some(head)) => diff_columns(base, head),
                        _ => vec![],
                    }
                }
                ModelDiffStatus::Added => {
                    // Show all columns as added for new models
                    change
                        .head_schema_name
                        .as_ref()
                        .and_then(|name| head_schemas.get(name))
                        .map(|cols| {
                            cols.iter()
                                .map(|c| ColumnDiff {
                                    column_name: c.name.clone(),
                                    change_type: ColumnChangeType::Added,
                                    old_type: None,
                                    new_type: Some(c.data_type.clone()),
                                })
                                .collect()
                        })
                        .unwrap_or_default()
                }
                ModelDiffStatus::Removed => {
                    // Show all columns as removed for deleted models
                    change
                        .base_schema_name
                        .as_ref()
                        .and_then(|name| base_schemas.get(name))
                        .map(|cols| {
                            cols.iter()
                                .map(|c| ColumnDiff {
                                    column_name: c.name.clone(),
                                    change_type: ColumnChangeType::Removed,
                                    old_type: Some(c.data_type.clone()),
                                    new_type: None,
                                })
                                .collect()
                        })
                        .unwrap_or_default()
                }
                ModelDiffStatus::Unchanged => vec![],
            };

            let result = DiffResult {
                model_name: change.model_name.clone(),
                status,
                // The change-map key IS the resolved target on the resolved
                // path. On the fallback path it is a filename stem, which is
                // not a warehouse identity — leave it unset rather than
                // publishing a stem as though it were one.
                resolved_target: change.resolved_target.clone(),
                row_count_before: None,
                row_count_after: None,
                column_changes,
                sample_changed_rows: None,
            };
            (key.as_str(), result)
        })
        .collect();

    // Sort by model name for deterministic output, then by the change map's
    // key to break ties. `model_name` alone is not a total order — a model
    // whose target moved appears twice under one name (removed + added) — and
    // a stable sort over randomized `HashMap` iteration would then leave the
    // order, and the Markdown rendered from it, varying between runs. Keys are
    // unique by construction, so this is total.
    results.sort_by(|a, b| {
        a.1.model_name
            .cmp(&b.1.model_name)
            .then_with(|| a.0.cmp(b.0))
    });
    results.into_iter().map(|(_, result)| result).collect()
}

// ---------------------------------------------------------------------------
// Shared diff computation
// ---------------------------------------------------------------------------

/// Result of computing a CI diff.
///
/// `head_compile` is `None` when the models directory is missing or the
/// HEAD compile fails — callers (e.g. `rocky lineage-diff`) that need the
/// `semantic_graph` for downstream traces must handle that gracefully.
///
/// `base_compile` is similarly `None` when the base ref can't be checked
/// out into a temp dir or fails to compile. Both are kept on the data
/// struct so the `--semantic` path can lower them into [`ProjectIr`]
/// without re-running the compiler.
pub(crate) struct CiDiffData {
    pub(crate) summary: DiffSummary,
    pub(crate) results: Vec<DiffResult>,
    pub(crate) head_compile: Option<rocky_compiler::compile::CompileResult>,
    pub(crate) base_compile: Option<rocky_compiler::compile::CompileResult>,
    /// Total count of files git reported as changed between `base_ref` and
    /// HEAD (any extension, before the model-file filter). Lets callers
    /// distinguish "PR is empty" from "PR is non-empty but only touches
    /// non-model files".
    pub(crate) changed_file_count: usize,
}

/// Compute the CI diff between `base_ref` and HEAD without printing.
///
/// Shared between `run_ci_diff` and `run_lineage_diff` so the lineage-diff
/// command can enrich the per-column diff with downstream traces from
/// HEAD's `semantic_graph` without rerunning git or the compiler.
pub(crate) fn compute_ci_diff(
    config_path: &Path,
    state_path: &Path,
    base_ref: &str,
    models_dir: &Path,
    cache_ttl_override: Option<u64>,
) -> Result<CiDiffData> {
    compute_ci_diff_in(
        config_path,
        state_path,
        base_ref,
        models_dir,
        cache_ttl_override,
        None,
    )
}

/// [`compute_ci_diff`], with the git invocations optionally rooted at an
/// explicit repository directory rather than the process cwd. See [`git_in`].
fn compute_ci_diff_in(
    config_path: &Path,
    state_path: &Path,
    base_ref: &str,
    models_dir: &Path,
    cache_ttl_override: Option<u64>,
    repo_dir: Option<&Path>,
) -> Result<CiDiffData> {
    validate_base_ref(base_ref)?;

    // Load cached source schemas once and seed both compiles (current
    // tree + base ref) with the same map so the resulting per-model
    // type diffs measure real schema drift rather than
    // `Unknown`-vs-`Unknown` noise. Degrades to empty when the cache is
    // cold or `[cache.schemas] enabled = false`.
    let source_schemas = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(cache_ttl_override);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, state_path)
        }
        Err(_) => HashMap::new(),
    };

    let changed_files = git_changed_files(base_ref, repo_dir)?;
    let changed_file_count = changed_files.len();
    if changed_files.is_empty() {
        return Ok(CiDiffData {
            summary: DiffSummary {
                total_models: 0,
                unchanged: 0,
                modified: 0,
                added: 0,
                removed: 0,
            },
            results: vec![],
            head_compile: None,
            base_compile: None,
            changed_file_count,
        });
    }

    let models_rel = find_models_relative_path(models_dir, repo_dir);
    // Skipping the compiles also skips the `--semantic` breaking-change
    // classifier, so an empty structural classification must not be the sole
    // reason to bail. Shared config that feeds the compiler without being a
    // model file — `_defaults.toml`, `groups/*.toml`, `test_definitions.toml`
    // — can retarget every model in the project while classifying as nothing
    // at all. Compile whenever anything under the models root moved, and let
    // the classifier decide there is no *structural* change.
    let touches_models_root = changed_files.iter().any(|file| {
        compiler_input_under_models_root(&file.path, models_rel.as_deref())
            || file
                .old_path
                .as_deref()
                .is_some_and(|path| compiler_input_under_models_root(path, models_rel.as_deref()))
    });
    if !touches_models_root
        && classify_model_changes(&changed_files, models_rel.as_deref(), None, None).is_empty()
    {
        return Ok(CiDiffData {
            summary: DiffSummary {
                total_models: 0,
                unchanged: 0,
                modified: 0,
                added: 0,
                removed: 0,
            },
            results: vec![],
            head_compile: None,
            base_compile: None,
            changed_file_count,
        });
    }

    // Compile HEAD: keep the full result so callers can reach into
    // `semantic_graph`. Schema extraction below is a cheap projection.
    let head_compile = if models_dir.is_dir() {
        match compile_head(models_dir, source_schemas.clone()) {
            Ok(r) => Some(r),
            Err(e) => {
                debug!("HEAD compilation failed: {e}");
                None
            }
        }
    } else {
        None
    };
    let head_schemas = head_compile
        .as_ref()
        .map(typed_columns_from_compile)
        .unwrap_or_default();

    let base_compile = if models_dir.is_dir() {
        extract_base_compile_in(base_ref, models_dir, source_schemas, repo_dir).ok()
    } else {
        None
    };
    let base_schemas = base_compile
        .as_ref()
        .map(typed_columns_from_compile)
        .unwrap_or_default();

    let model_changes = classify_model_changes(
        &changed_files,
        models_rel.as_deref(),
        base_compile
            .as_ref()
            .map(|result| result.project.models.as_slice()),
        head_compile
            .as_ref()
            .map(|result| result.project.models.as_slice()),
    );
    let results = build_diff_results(&model_changes, &head_schemas, &base_schemas);
    let summary = DiffSummary::from_results(&results);

    Ok(CiDiffData {
        summary,
        results,
        head_compile,
        base_compile,
        changed_file_count,
    })
}

// ---------------------------------------------------------------------------
// Semantic breaking-change lowering
// ---------------------------------------------------------------------------

/// Lower a [`rocky_compiler::compile::CompileResult`] into a
/// [`rocky_ir::ProjectIr`] suitable for the
/// [`rocky_core::breaking_change`] classifier.
///
/// Each model in `result.project.models` is converted via
/// [`rocky_core::models::Model::to_model_ir`] (which leaves
/// `typed_columns` empty) and then enriched with the typed columns from
/// `result.type_check.typed_models`, keyed by `config.name`. Models the
/// type-checker did not produce columns for keep their empty
/// `typed_columns` vec — the classifier handles this gracefully (it
/// just won't emit per-column findings for that model).
///
/// `dag` and `lineage_edges` are left empty: the classifier ignores both
/// (they are implementation-detail fields per the
/// `rocky_core::breaking_change` module docs).
pub fn project_ir_from_compile(
    result: &rocky_compiler::compile::CompileResult,
) -> rocky_ir::ProjectIr {
    let typed = &result.type_check.typed_models;
    let models = result
        .project
        .models
        .iter()
        .map(|m| {
            let mut ir = m.to_model_ir();
            if let Some(cols) = typed.get(&m.config.name) {
                ir.typed_columns = cols.clone();
            }
            ir
        })
        .collect();
    rocky_ir::ProjectIr {
        models,
        dag: Vec::new(),
        lineage_edges: Vec::new(),
    }
}

/// Run the semantic breaking-change classifier across `base` and `head`
/// compiles. Returns an empty vec when either side is `None`.
fn semantic_findings(
    base: Option<&rocky_compiler::compile::CompileResult>,
    head: Option<&rocky_compiler::compile::CompileResult>,
) -> Vec<rocky_core::breaking_change::BreakingFinding> {
    match (base, head) {
        (Some(b), Some(h)) => {
            let old = project_ir_from_compile(b);
            let new = project_ir_from_compile(h);
            rocky_core::breaking_change::diff_project_ir(&old, &new)
        }
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Public command entry point
// ---------------------------------------------------------------------------

/// Execute `rocky ci-diff`.
///
/// `semantic` enables the typed-IR breaking-change classifier
/// ([`rocky_core::breaking_change::diff_project_ir`]); findings are
/// attached to the JSON output under `breaking_findings`. The flag is
/// informational only: even a `Breaking` finding does not change the
/// exit code. The hard gate lives on `rocky branch promote`.
pub fn run_ci_diff(
    config_path: &Path,
    state_path: &Path,
    base_ref: &str,
    models_dir: &Path,
    output_json: bool,
    semantic: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let data = compute_ci_diff(
        config_path,
        state_path,
        base_ref,
        models_dir,
        cache_ttl_override,
    )?;

    // Classify before the empty-structural-diff branch below. A change to
    // shared config — `_defaults.toml`, `groups/*.toml` — retargets models
    // without touching any model file, so it produces no structural rows while
    // still being a breaking change. Reporting "nothing changed" there would
    // be a silent pass on the one surface asked to catch it.
    let findings = if semantic {
        semantic_findings(data.base_compile.as_ref(), data.head_compile.as_ref())
    } else {
        Vec::new()
    };

    if data.results.is_empty() && data.summary.total_models == 0 {
        // No model-level diff — distinguish "PR is empty" from "PR touched
        // non-model files only" the same way `rocky ci-diff` did before
        // the `compute_ci_diff` extraction.
        if output_json {
            let output = CiDiffOutput::new(
                base_ref.to_string(),
                "HEAD".to_string(),
                data.summary,
                vec![],
            )
            .with_breaking_findings(findings);
            print_json(&output)?;
        } else if data.changed_file_count == 0 {
            println!("Rocky CI Diff ({base_ref}...HEAD)\n");
            println!("No changed model files detected.");
        } else {
            println!("Rocky CI Diff ({base_ref}...HEAD)\n");
            println!(
                "{} file(s) changed, but no model files (.sql, .rocky) were affected.",
                data.changed_file_count,
            );
            print_semantic_findings(semantic, &findings);
        }
        return Ok(());
    }

    if output_json {
        let output = CiDiffOutput::new(
            base_ref.to_string(),
            "HEAD".to_string(),
            data.summary,
            data.results,
        )
        .with_breaking_findings(findings);
        print_json(&output)?;
    } else {
        println!("Rocky CI Diff ({base_ref}...HEAD)\n");
        print!("{}", format_diff_table(&data.results));
        println!();
        println!("--- Markdown (for PR comment) ---\n");
        // Render the same Markdown the JSON `markdown` field carries. This
        // block is what a human copies into the PR, so it is exactly the
        // surface that must not omit a breaking finding.
        print!(
            "{}",
            crate::output::markdown_with_findings(
                &format_diff_markdown(&data.results),
                data.summary.is_clean(),
                &findings,
            )
        );
    }

    Ok(())
}

/// Print the semantic breaking-change findings in the human-readable output.
///
/// Shared by the normal path and the empty-structural-diff path, which can
/// still carry findings when shared config retargeted a model.
fn print_semantic_findings(
    semantic: bool,
    findings: &[rocky_core::breaking_change::BreakingFinding],
) {
    if !semantic || findings.is_empty() {
        return;
    }
    println!();
    println!("--- Semantic Findings ({} total) ---\n", findings.len());
    for f in findings {
        let sev = match f.severity {
            rocky_core::breaking_change::BreakingSeverity::Breaking => "BREAKING",
            rocky_core::breaking_change::BreakingSeverity::Warning => "WARNING",
            rocky_core::breaking_change::BreakingSeverity::Info => "INFO",
        };
        println!("[{sev}] {:?}", f.change);
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn changed(path: &str, status: char) -> ChangedFile {
        ChangedFile {
            path: path.to_string(),
            old_path: None,
            status,
        }
    }

    fn model_change(name: &str, status: ModelDiffStatus) -> ModelChange {
        let (base_schema_name, head_schema_name) = match status {
            ModelDiffStatus::Added => (None, Some(name.to_string())),
            ModelDiffStatus::Removed => (Some(name.to_string()), None),
            ModelDiffStatus::Modified => (Some(name.to_string()), Some(name.to_string())),
            ModelDiffStatus::Unchanged => (None, None),
        };
        ModelChange {
            model_name: name.to_string(),
            base_schema_name,
            head_schema_name,
            resolved_target: None,
            ..Default::default()
        }
    }

    #[test]
    fn validate_base_ref_rejects_option_injection() {
        // A leading-dash ref would otherwise smuggle flags into the git
        // invocations (e.g. via ${{ github.base_ref }} in CI).
        assert!(validate_base_ref("--upload-pack=touch /tmp/pwned").is_err());
        assert!(validate_base_ref("-foo").is_err());
        assert!(validate_base_ref("").is_err());
        // Legitimate revisions still pass.
        assert!(validate_base_ref("origin/main").is_ok());
        assert!(validate_base_ref("HEAD~3").is_ok());
        assert!(validate_base_ref("abc1234").is_ok());
    }

    // -----------------------------------------------------------------------
    // parse_name_status
    // -----------------------------------------------------------------------

    #[test]
    fn parse_empty_output() {
        let files = parse_name_status(b"").unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn parse_added_modified_deleted() {
        let raw = b"A\tmodels/orders.sql\nM\tmodels/customers.sql\nD\tmodels/legacy.sql\n";
        let files = parse_name_status(raw).unwrap();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].status, 'A');
        assert_eq!(files[0].path, "models/orders.sql");
        assert_eq!(files[1].status, 'M');
        assert_eq!(files[1].path, "models/customers.sql");
        assert_eq!(files[2].status, 'D');
        assert_eq!(files[2].path, "models/legacy.sql");
    }

    #[test]
    fn parse_rename_preserves_both_paths() {
        let raw = b"R100\tmodels/old_name.sql\tmodels/new_name.sql\n";
        let files = parse_name_status(raw).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].status, 'R');
        assert_eq!(files[0].path, "models/new_name.sql");
        assert_eq!(files[0].old_path.as_deref(), Some("models/old_name.sql"));
    }

    #[test]
    fn parse_skips_blank_lines() {
        let raw = b"M\tmodels/foo.sql\n\n\nA\tmodels/bar.sql\n";
        let files = parse_name_status(raw).unwrap();
        assert_eq!(files.len(), 2);
    }

    // -----------------------------------------------------------------------
    // classify_model_changes
    // -----------------------------------------------------------------------

    #[test]
    fn classify_sql_files() {
        let files = vec![
            changed("models/orders.sql", 'A'),
            changed("models/customers.sql", 'M'),
            changed("models/legacy.sql", 'D'),
        ];
        let changes = classify_model_changes(&files, Some("models"), None, None);
        assert_eq!(changes["orders"].status(), ModelDiffStatus::Added);
        assert_eq!(changes["customers"].status(), ModelDiffStatus::Modified);
        assert_eq!(changes["legacy"].status(), ModelDiffStatus::Removed);
    }

    #[test]
    fn classify_rocky_files() {
        let files = vec![changed("models/pipeline.rocky", 'A')];
        let changes = classify_model_changes(&files, Some("models"), None, None);
        assert_eq!(changes["pipeline"].status(), ModelDiffStatus::Added);
    }

    #[test]
    fn classify_toml_sidecars() {
        let files = vec![changed("models/orders.toml", 'M')];
        let changes = classify_model_changes(&files, Some("models"), None, None);
        assert_eq!(changes["orders"].status(), ModelDiffStatus::Modified);
    }

    #[test]
    fn classify_ignores_non_model_files() {
        let files = vec![
            changed("rocky.toml", 'M'),
            changed("Cargo.toml", 'M'),
            changed("models/_defaults.toml", 'M'),
            changed("models/groups/orders.toml", 'M'),
            changed("README.md", 'M'),
            changed("src/main.rs", 'M'),
        ];
        let changes = classify_model_changes(&files, Some("models"), None, None);
        assert!(changes.is_empty());
    }

    #[test]
    fn classify_combined_sql_and_toml_prefers_significant() {
        // When both .sql (Added) and .toml (Modified) change for the same model,
        // the more significant status (Added) should win.
        let files = vec![
            changed("models/orders.toml", 'M'),
            changed("models/orders.sql", 'A'),
        ];
        let changes = classify_model_changes(&files, Some("models"), None, None);
        assert_eq!(changes["orders"].status(), ModelDiffStatus::Added);
    }

    #[test]
    fn classify_copy_as_added() {
        let files = parse_name_status(b"C100\tmodels/orders.sql\tmodels/purchases.sql\n").unwrap();
        let changes = classify_model_changes(&files, Some("models"), None, None);
        assert_eq!(changes["purchases"].status(), ModelDiffStatus::Added);
    }

    /// A copy leaves its source untouched, so on the fallback path the old
    /// path must not lend its stem to a destination outside the models root —
    /// that would report an existing model as newly Added.
    #[test]
    fn classify_copy_out_of_models_ignores_the_untouched_source() {
        let files = parse_name_status(b"C100\tmodels/orders.sql\tarchive/orders.sql\n").unwrap();
        let changes = classify_model_changes(&files, Some("models"), None, None);
        assert!(
            changes.is_empty(),
            "copying a model out of the models root changes no model: {changes:?}"
        );
    }

    #[test]
    fn classify_rename_out_of_models_uses_old_path() {
        let files = parse_name_status(b"R100\tmodels/orders.sql\tarchive/orders.sql\n").unwrap();
        let changes = classify_model_changes(&files, Some("models"), None, None);
        assert_eq!(changes["orders"].status(), ModelDiffStatus::Modified);
    }

    // -----------------------------------------------------------------------
    // diff_columns
    // -----------------------------------------------------------------------

    #[test]
    fn diff_columns_no_changes() {
        let base = vec![
            TypedColumn {
                name: "id".into(),
                data_type: "INT".into(),
            },
            TypedColumn {
                name: "name".into(),
                data_type: "VARCHAR".into(),
            },
        ];
        let diffs = diff_columns(&base, &base);
        assert!(diffs.is_empty());
    }

    #[test]
    fn diff_columns_added() {
        let base = vec![TypedColumn {
            name: "id".into(),
            data_type: "INT".into(),
        }];
        let head = vec![
            TypedColumn {
                name: "id".into(),
                data_type: "INT".into(),
            },
            TypedColumn {
                name: "email".into(),
                data_type: "VARCHAR".into(),
            },
        ];
        let diffs = diff_columns(&base, &head);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].column_name, "email");
        assert_eq!(diffs[0].change_type, ColumnChangeType::Added);
        assert_eq!(diffs[0].new_type, Some("VARCHAR".into()));
    }

    #[test]
    fn diff_columns_removed() {
        let base = vec![
            TypedColumn {
                name: "id".into(),
                data_type: "INT".into(),
            },
            TypedColumn {
                name: "legacy_flag".into(),
                data_type: "BOOLEAN".into(),
            },
        ];
        let head = vec![TypedColumn {
            name: "id".into(),
            data_type: "INT".into(),
        }];
        let diffs = diff_columns(&base, &head);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].column_name, "legacy_flag");
        assert_eq!(diffs[0].change_type, ColumnChangeType::Removed);
        assert_eq!(diffs[0].old_type, Some("BOOLEAN".into()));
    }

    #[test]
    fn diff_columns_type_changed() {
        let base = vec![TypedColumn {
            name: "price".into(),
            data_type: "FLOAT".into(),
        }];
        let head = vec![TypedColumn {
            name: "price".into(),
            data_type: "DOUBLE".into(),
        }];
        let diffs = diff_columns(&base, &head);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].column_name, "price");
        assert_eq!(diffs[0].change_type, ColumnChangeType::TypeChanged);
        assert_eq!(diffs[0].old_type, Some("FLOAT".into()));
        assert_eq!(diffs[0].new_type, Some("DOUBLE".into()));
    }

    #[test]
    fn diff_columns_mixed() {
        let base = vec![
            TypedColumn {
                name: "id".into(),
                data_type: "INT".into(),
            },
            TypedColumn {
                name: "old_col".into(),
                data_type: "TEXT".into(),
            },
            TypedColumn {
                name: "amount".into(),
                data_type: "FLOAT".into(),
            },
        ];
        let head = vec![
            TypedColumn {
                name: "id".into(),
                data_type: "INT".into(),
            },
            TypedColumn {
                name: "amount".into(),
                data_type: "DECIMAL".into(),
            },
            TypedColumn {
                name: "new_col".into(),
                data_type: "VARCHAR".into(),
            },
        ];
        let diffs = diff_columns(&base, &head);
        assert_eq!(diffs.len(), 3);

        let added: Vec<_> = diffs
            .iter()
            .filter(|d| d.change_type == ColumnChangeType::Added)
            .collect();
        let removed: Vec<_> = diffs
            .iter()
            .filter(|d| d.change_type == ColumnChangeType::Removed)
            .collect();
        let changed: Vec<_> = diffs
            .iter()
            .filter(|d| d.change_type == ColumnChangeType::TypeChanged)
            .collect();

        assert_eq!(added.len(), 1);
        assert_eq!(added[0].column_name, "new_col");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].column_name, "old_col");
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].column_name, "amount");
    }

    // -----------------------------------------------------------------------
    // build_diff_results
    // -----------------------------------------------------------------------

    #[test]
    fn build_results_sorts_by_name() {
        let mut model_changes = HashMap::new();
        model_changes.insert(
            "c.s.zebra".into(),
            model_change("zebra", ModelDiffStatus::Added),
        );
        model_changes.insert(
            "c.s.alpha".into(),
            model_change("alpha", ModelDiffStatus::Modified),
        );

        let results = build_diff_results(&model_changes, &HashMap::new(), &HashMap::new());
        assert_eq!(results[0].model_name, "alpha");
        assert_eq!(results[1].model_name, "zebra");
    }

    #[test]
    fn build_results_with_schemas() {
        let mut model_changes = HashMap::new();
        model_changes.insert(
            "c.s.orders".into(),
            model_change("orders", ModelDiffStatus::Modified),
        );

        let mut base_schemas = HashMap::new();
        base_schemas.insert(
            "orders".into(),
            vec![
                TypedColumn {
                    name: "id".into(),
                    data_type: "INT".into(),
                },
                TypedColumn {
                    name: "price".into(),
                    data_type: "FLOAT".into(),
                },
            ],
        );

        let mut head_schemas = HashMap::new();
        head_schemas.insert(
            "orders".into(),
            vec![
                TypedColumn {
                    name: "id".into(),
                    data_type: "INT".into(),
                },
                TypedColumn {
                    name: "price".into(),
                    data_type: "DOUBLE".into(),
                },
                TypedColumn {
                    name: "tax".into(),
                    data_type: "DECIMAL".into(),
                },
            ],
        );

        let results = build_diff_results(&model_changes, &head_schemas, &base_schemas);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].model_name, "orders");
        assert_eq!(results[0].status, ModelDiffStatus::Modified);
        assert_eq!(results[0].column_changes.len(), 2); // price type-changed + tax added
    }

    #[test]
    fn build_results_added_model_shows_all_columns() {
        let mut model_changes = HashMap::new();
        model_changes.insert(
            "c.s.new_model".into(),
            model_change("new_model", ModelDiffStatus::Added),
        );

        let mut head_schemas = HashMap::new();
        head_schemas.insert(
            "new_model".into(),
            vec![
                TypedColumn {
                    name: "id".into(),
                    data_type: "INT".into(),
                },
                TypedColumn {
                    name: "name".into(),
                    data_type: "VARCHAR".into(),
                },
            ],
        );

        let results = build_diff_results(&model_changes, &head_schemas, &HashMap::new());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].column_changes.len(), 2);
        assert!(
            results[0]
                .column_changes
                .iter()
                .all(|c| c.change_type == ColumnChangeType::Added)
        );
    }

    #[test]
    fn build_results_removed_model_shows_all_columns() {
        let mut model_changes = HashMap::new();
        model_changes.insert(
            "c.s.old_model".into(),
            model_change("old_model", ModelDiffStatus::Removed),
        );

        let mut base_schemas = HashMap::new();
        base_schemas.insert(
            "old_model".into(),
            vec![TypedColumn {
                name: "id".into(),
                data_type: "INT".into(),
            }],
        );

        let results = build_diff_results(&model_changes, &HashMap::new(), &base_schemas);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].column_changes.len(), 1);
        assert_eq!(
            results[0].column_changes[0].change_type,
            ColumnChangeType::Removed
        );
    }

    // -----------------------------------------------------------------------
    // project_ir_from_compile + semantic_findings (semantic mode stitching)
    // -----------------------------------------------------------------------

    use std::fs;
    use tempfile::TempDir;

    /// Write a minimal transformation model: `<name>.sql` + sidecar
    /// `<name>.toml`. Mirrors the test helper in `compile.rs`.
    /// #1237: when only ONE ref compiles, a row still carries the target that
    /// side knew — status classification is unchanged.
    ///
    /// The both-compiled path took the resolved route; otherwise every row
    /// hard-coded `resolved_target: None`. That was lossiness, not a wrong
    /// answer: a PR adding the first model to a project has a HEAD compile
    /// that knows the target, and the empty base cannot compile at all.
    #[test]
    fn a_single_sided_compile_still_resolves_the_target_it_knew() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        fs::create_dir_all(&models).unwrap();
        write_model(&models, "orders", "SELECT 1 AS id");
        let head = compile_head(&models, HashMap::new()).expect("head compiles");
        let head_models: Vec<Model> = head.project.models.clone();

        // HEAD-only: the added row takes HEAD's target.
        let added = classify_model_changes(
            &[changed("models/orders.sql", 'A')],
            Some("models"),
            None,
            Some(&head_models),
        );
        assert_eq!(
            added.get("orders").and_then(|c| c.resolved_target.clone()),
            Some("c.s.orders".to_string()),
            "an Added row must take the target from the side that compiled"
        );
        assert_eq!(added["orders"].base_schema_name, None);
        assert_eq!(
            added["orders"].head_schema_name.as_deref(),
            Some("orders"),
            "status classification must be unchanged by the enrichment"
        );

        // Base-only: the removed row takes base's target.
        let removed = classify_model_changes(
            &[changed("models/orders.sql", 'D')],
            Some("models"),
            Some(&head_models),
            None,
        );
        assert_eq!(
            removed
                .get("orders")
                .and_then(|c| c.resolved_target.clone()),
            Some("c.s.orders".to_string()),
            "a Removed row must take the target from the base compile"
        );
    }

    /// #1237, red-team finding: a path with MORE THAN ONE owning model must
    /// not publish an arbitrary one of their targets.
    ///
    /// `orders.contract.toml` is the ordinary sidecar of `orders.contract.sql`
    /// *and* the contract of `orders.sql`; `model_owns_file` deliberately
    /// matches both (see `ownership_resolves_contract_stem_and_contract_sidecar`).
    /// Taking the first match would publish one model's warehouse target under
    /// a row naming the other — a wrong answer, not merely a missing one,
    /// against a field documented as "the warehouse object this row describes".
    #[test]
    fn an_ambiguously_owned_path_publishes_no_target() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        fs::create_dir_all(&models).unwrap();

        // Two models whose ownership of `orders.contract.toml` overlaps, with
        // DIFFERENT targets — so picking either one is observably wrong.
        write_model_with_identity(
            &models,
            "orders",
            "orders",
            "orders",
            "SELECT id FROM src_orders",
        );
        write_model_with_identity(
            &models,
            "orders.contract",
            "orders_contract",
            "orders_contract",
            "SELECT id FROM src_orders",
        );
        let compile = compile_head(&models, sources).expect("compile");
        let head_models: Vec<Model> = compile.project.models.clone();

        let changes = classify_model_changes(
            &[changed("models/orders.contract.toml", 'A')],
            Some("models"),
            None,
            Some(&head_models),
        );

        assert_eq!(
            changes
                .get("orders")
                .and_then(|c| c.resolved_target.clone()),
            None,
            "two models own this path with different targets, so neither may be \
             published as the object this row describes"
        );
    }

    /// An owner whose target is not a publishable identifier must not be
    /// silently dropped from the ambiguity check.
    ///
    /// Filtering unpublishable owners out BEFORE testing uniqueness would
    /// leave exactly one survivor for a two-owner path, making it look unique
    /// and publishing a target for a row whose ownership was never
    /// established. The ambiguity is about which model the row describes, not
    /// about whether one of them happens to be nameable.
    #[test]
    fn an_unidentifiable_co_owner_still_blocks_the_target() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        fs::create_dir_all(&models).unwrap();

        // `orders` has a perfectly publishable target; its co-owner of
        // `orders.contract.toml` does not (a hyphen is not a SQL identifier).
        write_model_with_identity(
            &models,
            "orders",
            "orders",
            "orders",
            "SELECT id FROM src_orders",
        );
        write_model_with_identity(
            &models,
            "orders.contract",
            "orders_contract",
            "not-an-identifier",
            "SELECT id FROM src_orders",
        );
        let compile = compile_head(&models, sources).expect("compile");
        let head_models: Vec<Model> = compile.project.models.clone();

        let changes = classify_model_changes(
            &[changed("models/orders.contract.toml", 'A')],
            Some("models"),
            None,
            Some(&head_models),
        );

        assert_eq!(
            changes
                .get("orders")
                .and_then(|c| c.resolved_target.clone()),
            None,
            "an unidentifiable co-owner must withhold the row, not be filtered \
             out until the remaining owner looks unique"
        );
    }

    /// The enrichment must not cross sides: a `Removed` row cannot borrow a
    /// target from a HEAD-only compile, because the model it describes is
    /// precisely the one HEAD no longer has.
    #[test]
    fn a_single_sided_compile_does_not_supply_the_wrong_sides_target() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        fs::create_dir_all(&models).unwrap();
        write_model(&models, "orders", "SELECT 1 AS id");
        let head = compile_head(&models, HashMap::new()).expect("head compiles");
        let head_models: Vec<Model> = head.project.models.clone();

        let removed = classify_model_changes(
            &[changed("models/orders.sql", 'D')],
            Some("models"),
            None,
            Some(&head_models),
        );
        assert_eq!(
            removed
                .get("orders")
                .and_then(|c| c.resolved_target.clone()),
            None,
            "a Removed row must not take a target from a HEAD-only compile"
        );
    }

    fn write_model(dir: &Path, name: &str, sql: &str) {
        write_model_with_identity(dir, name, name, name, sql);
    }

    fn write_model_with_identity(
        dir: &Path,
        file_stem: &str,
        name: &str,
        target_table: &str,
        sql: &str,
    ) {
        let sql_path = dir.join(format!("{file_stem}.sql"));
        let toml_path = dir.join(format!("{file_stem}.toml"));
        fs::write(&sql_path, sql).unwrap();
        fs::write(
            &toml_path,
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{target_table}\"\n"
            ),
        )
        .unwrap();
    }

    /// Like `write_model_with_identity` but the schema is caller-chosen, so a
    /// fixture can move a model to a different target.
    fn write_model_at(dir: &Path, file_stem: &str, name: &str, schema: &str, table: &str) {
        fs::write(
            dir.join(format!("{file_stem}.sql")),
            "SELECT id FROM src_orders",
        )
        .unwrap();
        fs::write(
            dir.join(format!("{file_stem}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"{schema}\"\ntable = \"{table}\"\n"
            ),
        )
        .unwrap();
    }

    fn write_inferred_model(dir: &Path, file_stem: &str, sql: &str) {
        fs::write(dir.join(format!("{file_stem}.sql")), sql).unwrap();
        fs::write(
            dir.join(format!("{file_stem}.toml")),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\n",
        )
        .unwrap();
    }

    /// Build a `HashMap<source_name, Vec<TypedColumn>>` to seed the
    /// compiler so SELECT FROM <source> yields concrete typed columns.
    fn source_schema(
        name: &str,
        cols: &[(&str, rocky_ir::RockyType)],
    ) -> HashMap<String, Vec<rocky_compiler::types::TypedColumn>> {
        let mut map = HashMap::new();
        map.insert(
            name.to_string(),
            cols.iter()
                .map(|(n, t)| rocky_compiler::types::TypedColumn {
                    name: (*n).to_string(),
                    data_type: t.clone(),
                    nullable: true,
                })
                .collect(),
        );
        map
    }

    #[test]
    fn model_stem_normalizes_contract_and_rejects_nested_config() {
        assert_eq!(
            model_stem("models/orders.contract.toml", Some("models")).as_deref(),
            Some("orders")
        );
        assert_eq!(
            model_stem("models/groups/orders.toml", Some("models")),
            None
        );
        assert_eq!(
            model_stem("models/orders.sql", None).as_deref(),
            Some("orders")
        );
    }

    #[test]
    fn filename_inferred_rename_is_removed_and_added() {
        let sources = source_schema(
            "src_orders",
            &[
                ("id", rocky_ir::RockyType::Int64),
                ("status", rocky_ir::RockyType::String),
            ],
        );
        let base_dir = TempDir::new().unwrap();
        write_inferred_model(
            base_dir.path(),
            "orders",
            "SELECT id, status FROM src_orders",
        );
        let head_dir = TempDir::new().unwrap();
        write_inferred_model(
            head_dir.path(),
            "purchases",
            "SELECT id, status FROM src_orders",
        );
        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");
        let files = parse_name_status(
            b"R100\tmodels/orders.sql\tmodels/purchases.sql\n\
              R100\tmodels/orders.toml\tmodels/purchases.toml\n\
              R100\tmodels/orders.contract.toml\tmodels/purchases.contract.toml\n",
        )
        .unwrap();

        let changes = classify_model_changes(
            &files,
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );
        assert_eq!(changes.len(), 2);
        assert_eq!(changes["c.s.orders"].status(), ModelDiffStatus::Removed);
        assert_eq!(changes["c.s.purchases"].status(), ModelDiffStatus::Added);

        let results = build_diff_results(
            &changes,
            &typed_columns_from_compile(&head_compile),
            &typed_columns_from_compile(&base_compile),
        );
        let removed = results
            .iter()
            .find(|result| result.status == ModelDiffStatus::Removed)
            .unwrap();
        assert_eq!(removed.model_name, "orders");
        assert_eq!(removed.column_changes.len(), 2);
        assert!(
            removed
                .column_changes
                .iter()
                .all(|column| column.change_type == ColumnChangeType::Removed)
        );
        let added = results
            .iter()
            .find(|result| result.status == ModelDiffStatus::Added)
            .unwrap();
        assert_eq!(added.model_name, "purchases");
        assert_eq!(added.column_changes.len(), 2);
        assert!(
            added
                .column_changes
                .iter()
                .all(|column| column.change_type == ColumnChangeType::Added)
        );
    }

    #[test]
    fn rename_with_stable_target_is_modified_and_fallback_stays_conservative() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let base_dir = TempDir::new().unwrap();
        write_model_with_identity(
            base_dir.path(),
            "orders_v1",
            "old_orders",
            "orders",
            "SELECT id FROM src_orders",
        );
        let head_dir = TempDir::new().unwrap();
        write_model_with_identity(
            head_dir.path(),
            "orders_v2",
            "new_orders",
            "orders",
            "SELECT id FROM src_orders",
        );
        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");
        let files = parse_name_status(
            b"R100\tmodels/orders_v1.sql\tmodels/orders_v2.sql\n\
              R100\tmodels/orders_v1.toml\tmodels/orders_v2.toml\n",
        )
        .unwrap();

        let changes = classify_model_changes(
            &files,
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );
        assert_eq!(changes.len(), 1);
        let change = &changes["c.s.orders"];
        assert_eq!(change.status(), ModelDiffStatus::Modified);
        assert_eq!(change.model_name, "new_orders");
        assert_eq!(change.base_schema_name.as_deref(), Some("old_orders"));
        assert_eq!(change.head_schema_name.as_deref(), Some("new_orders"));

        let results = build_diff_results(
            &changes,
            &typed_columns_from_compile(&head_compile),
            &typed_columns_from_compile(&base_compile),
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].status, ModelDiffStatus::Modified);
        assert!(results[0].column_changes.is_empty());

        let fallback = classify_model_changes(
            &files,
            Some("models"),
            None,
            Some(&head_compile.project.models),
        );
        assert_eq!(fallback.len(), 1);
        assert_eq!(fallback["orders_v2"].status(), ModelDiffStatus::Modified);
    }

    /// #1236: two models share a target and one moves away. The vacated
    /// target is still written by the other, so nothing was abandoned —
    /// reporting it `removed` tells a reader to drop a live table.
    ///
    /// This shape is exactly a PR that *fixes* an E036 duplicate-target
    /// collision, so it is a change worth making, not a broken state to
    /// refuse.
    #[test]
    fn retargeting_one_of_a_colliding_pair_leaves_the_shared_target_occupied() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let base_dir = TempDir::new().unwrap();
        write_model_at(base_dir.path(), "a", "a", "s", "orders");
        write_model_at(base_dir.path(), "b", "b", "s", "orders");
        let head_dir = TempDir::new().unwrap();
        write_model_at(head_dir.path(), "a", "a", "s2", "orders");
        write_model_at(head_dir.path(), "b", "b", "s", "orders");

        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");
        let files = parse_name_status(b"M\tmodels/a.toml\n").unwrap();

        let changes = classify_model_changes(
            &files,
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );

        assert!(
            changes
                .values()
                .all(|change| change.status() != ModelDiffStatus::Removed),
            "c.s.orders is still written by `b`, so no row may claim a removal: {:?}",
            changes
                .iter()
                .map(|(k, v)| (k, v.status()))
                .collect::<Vec<_>>()
        );

        let shared = changes
            .values()
            .find(|change| change.resolved_target.as_deref() == Some("c.s.orders"))
            .expect("the vacated target still has a row");
        assert_eq!(shared.status(), ModelDiffStatus::Modified);
        // Deliberately NOT `Some("b")`. The row keeps its own `model_name`,
        // and `lineage-diff` traces columns through that name, so borrowing
        // the occupant's schema would publish `b`'s columns under `a`'s
        // identity. It is also not well founded: a base target written by two
        // colliding models had no uniquely defined schema to diff against.
        assert_eq!(shared.head_schema_name, None);
        assert!(shared.head_target_occupied);

        let moved = changes
            .values()
            .find(|change| change.resolved_target.as_deref() == Some("c.s2.orders"))
            .expect("the new target has a row");
        assert_eq!(moved.status(), ModelDiffStatus::Added);
    }

    /// Occupancy uses the compiler's physical identity, not the row's raw
    /// spelling.
    ///
    /// `C.S.Orders` and `c.s.orders` may be one warehouse object, and E036
    /// already treats them as one. If occupancy did not, a surviving occupant
    /// spelled one way would fail to cover a target vacated in the other, and
    /// the row would still claim a removal — the exact failure #1236 reports,
    /// reached through a spelling difference instead.
    #[test]
    fn occupancy_folds_case_the_way_the_compiler_does() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let base_dir = TempDir::new().unwrap();
        write_model_at(base_dir.path(), "a", "a", "S", "Orders");
        write_model_at(base_dir.path(), "b", "b", "s", "orders");
        let head_dir = TempDir::new().unwrap();
        write_model_at(head_dir.path(), "a", "a", "s2", "orders");
        write_model_at(head_dir.path(), "b", "b", "s", "orders");

        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");
        let files = parse_name_status(b"M\tmodels/a.toml\n").unwrap();

        let changes = classify_model_changes(
            &files,
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );

        let vacated = changes
            .values()
            .find(|change| change.resolved_target.as_deref() == Some("c.S.Orders"))
            .expect("the vacated spelling still has a row");
        assert_eq!(
            vacated.status(),
            ModelDiffStatus::Modified,
            "`b` at c.s.orders is the same physical object as c.S.Orders, so it \
             covers the vacated target"
        );
    }

    /// A row must never pair one model's base side with a *different*
    /// model's head side.
    ///
    /// Base `a -> c.S.Orders` and `b -> c.s.orders`; head normalizes `a` onto
    /// `c.s.orders` and moves `b` away. Keyed on the raw spelling, neither
    /// side has a duplicate, so nothing is name-qualified — and base `b`
    /// pairs with head `a` under `c.s.orders`, comparing `b`'s base schema
    /// against `a`'s head schema. A column base `a` already supplied at that
    /// physical target then reads as newly added.
    ///
    /// Keyed on the physical object, both producers are seen to contest one
    /// table, so both rows are name-qualified — one row per producer, which
    /// is what that qualification exists for, and each row compares a model
    /// against itself.
    #[test]
    fn a_row_never_pairs_one_model_with_another_models_head_side() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let base_dir = TempDir::new().unwrap();
        write_model_at(base_dir.path(), "a", "a", "S", "Orders");
        write_model_at(base_dir.path(), "b", "b", "s", "orders");
        let head_dir = TempDir::new().unwrap();
        write_model_at(head_dir.path(), "a", "a", "s", "orders");
        write_model_at(head_dir.path(), "b", "b", "s2", "orders");

        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");
        let files = parse_name_status(b"M\tmodels/a.toml\nM\tmodels/b.toml\n").unwrap();

        let changes = classify_model_changes(
            &files,
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );

        for (key, change) in &changes {
            if let (Some(base), Some(head)) = (&change.base_schema_name, &change.head_schema_name) {
                assert_eq!(
                    base, head,
                    "row {key} compares base model {base} against head model {head}; \
                     a row describes one target, not two producers spliced together"
                );
            }
        }

        // And the vacated side is still reported as surviving, not removed:
        // `a` now writes the table `b` left.
        assert!(
            changes
                .values()
                .all(|change| change.status() != ModelDiffStatus::Removed),
            "c.s.orders is still written by `a`: {:?}",
            changes
                .iter()
                .map(|(k, v)| (k, v.status()))
                .collect::<Vec<_>>()
        );
    }

    /// An `ephemeral` model cannot keep a table alive.
    ///
    /// Its target is populated but phantom and nothing is ever materialized
    /// there — which is exactly why `collide_on_target` excludes ephemerals
    /// from E036. Counting one as an occupant would turn a genuine
    /// abandonment into `Modified`, hiding that a real table was left behind.
    #[test]
    fn an_ephemeral_model_does_not_keep_a_vacated_target_occupied() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let write_ephemeral = |dir: &Path, stem: &str, name: &str, table: &str| {
            fs::write(dir.join(format!("{stem}.sql")), "SELECT id FROM src_orders").unwrap();
            fs::write(
                dir.join(format!("{stem}.toml")),
                format!(
                    "name = \"{name}\"\n\n[strategy]\ntype = \"ephemeral\"\n\n\
                     [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{table}\"\n"
                ),
            )
            .unwrap();
        };

        let base_dir = TempDir::new().unwrap();
        write_model_at(base_dir.path(), "a", "a", "s", "orders");
        write_ephemeral(base_dir.path(), "e", "e", "orders");
        let head_dir = TempDir::new().unwrap();
        write_model_at(head_dir.path(), "a", "a", "s2", "orders");
        write_ephemeral(head_dir.path(), "e", "e", "orders");

        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");
        let files = parse_name_status(b"M\tmodels/a.toml\n").unwrap();

        let changes = classify_model_changes(
            &files,
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );

        let vacated = changes
            .values()
            .find(|change| change.resolved_target.as_deref() == Some("c.s.orders"))
            .expect("the vacated target still has a row");
        assert_eq!(
            vacated.status(),
            ModelDiffStatus::Removed,
            "only an ephemeral model is left at c.s.orders, and it materializes \
             nothing — the table really is abandoned"
        );
    }

    /// The symmetric direction, which #1236 only implies: adding a second
    /// model at a target that already existed must not report that target
    /// `Added`. The table is not new — this PR is *introducing* an E036
    /// collision on it, which is the opposite problem and equally worth
    /// describing accurately.
    #[test]
    fn adding_a_second_model_at_an_existing_target_is_not_an_addition() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let base_dir = TempDir::new().unwrap();
        write_model_at(base_dir.path(), "a", "a", "s", "orders");
        let head_dir = TempDir::new().unwrap();
        write_model_at(head_dir.path(), "a", "a", "s", "orders");
        write_model_at(head_dir.path(), "b", "b", "s", "orders");

        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");
        let files = parse_name_status(b"A\tmodels/b.sql\nA\tmodels/b.toml\n").unwrap();

        let changes = classify_model_changes(
            &files,
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );

        assert!(
            changes
                .values()
                .all(|change| change.status() != ModelDiffStatus::Added),
            "c.s.orders already existed in base, so no row may call it added: {:?}",
            changes
                .iter()
                .map(|(k, v)| (k, v.status()))
                .collect::<Vec<_>>()
        );
    }

    /// Several survivors, and still not a removal.
    ///
    /// No schema name is ever taken from an occupant (see the pass), so the
    /// flags are the only thing standing between this row and a false
    /// `removed`. Two models continue to write the target after the third
    /// moves away; the table is plainly not abandoned.
    #[test]
    fn a_target_with_several_surviving_occupants_is_occupied_without_a_name() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let base_dir = TempDir::new().unwrap();
        for name in ["a", "b", "c"] {
            write_model_at(base_dir.path(), name, name, "s", "orders");
        }
        let head_dir = TempDir::new().unwrap();
        write_model_at(head_dir.path(), "a", "a", "s2", "orders");
        write_model_at(head_dir.path(), "b", "b", "s", "orders");
        write_model_at(head_dir.path(), "c", "c", "s", "orders");

        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");
        let files = parse_name_status(b"M\tmodels/a.toml\n").unwrap();

        let changes = classify_model_changes(
            &files,
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );

        let shared = changes
            .values()
            .find(|change| change.resolved_target.as_deref() == Some("c.s.orders"))
            .expect("the vacated target still has a row");
        assert_eq!(
            shared.status(),
            ModelDiffStatus::Modified,
            "`b` and `c` both still write c.s.orders, so it is not removed"
        );
        assert_eq!(
            shared.head_schema_name, None,
            "two occupants means no single model defines the target, so the row \
             must not claim one — it records occupancy instead"
        );
        assert!(shared.head_target_occupied);
    }

    /// The control, and the reason this is occupancy rather than blanket
    /// suppression: when the vacated target has no other occupant it really
    /// is abandoned, and must still report `removed`.
    #[test]
    fn retargeting_a_sole_occupant_still_reports_the_old_target_removed() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let base_dir = TempDir::new().unwrap();
        write_model_at(base_dir.path(), "a", "a", "s", "a_tbl");
        write_model_at(base_dir.path(), "b", "b", "s", "b_tbl");
        let head_dir = TempDir::new().unwrap();
        write_model_at(head_dir.path(), "a", "a", "s2", "a_tbl");
        write_model_at(head_dir.path(), "b", "b", "s", "b_tbl");

        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");
        let files = parse_name_status(b"M\tmodels/a.toml\n").unwrap();

        let changes = classify_model_changes(
            &files,
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );

        let removed = changes
            .values()
            .find(|change| change.status() == ModelDiffStatus::Removed)
            .expect("an abandoned target must still report removed");
        assert_eq!(removed.resolved_target.as_deref(), Some("c.s.a_tbl"));
        assert!(
            changes
                .values()
                .any(|change| change.status() == ModelDiffStatus::Added
                    && change.resolved_target.as_deref() == Some("c.s2.a_tbl"))
        );
    }

    // -----------------------------------------------------------------------
    // Ownership resolution: filename inference is not enough
    // -----------------------------------------------------------------------

    /// `_defaults` / `rocky` / `Cargo` name shared config only as TOML. The
    /// model loaders accept any flat `.sql` / `.rocky` file, so excluding
    /// those stems at every extension hides real models.
    #[test]
    fn reserved_stems_are_excluded_only_as_toml() {
        for stem in ["_defaults", "rocky", "Cargo"] {
            assert_eq!(
                model_stem(&format!("models/{stem}.toml"), Some("models")),
                None,
                "{stem}.toml is shared config, not a model"
            );
            assert_eq!(
                model_stem(&format!("models/{stem}.sql"), Some("models")).as_deref(),
                Some(stem),
                "{stem}.sql is an ordinary model"
            );
            assert_eq!(
                model_stem(&format!("models/{stem}.rocky"), Some("models")).as_deref(),
                Some(stem),
                "{stem}.rocky is an ordinary model"
            );
        }
    }

    /// `orders.contract.toml` is ambiguous by name alone: it is the ordinary
    /// sidecar of `orders.contract.sql` *and* the contract of `orders.sql`.
    /// Ownership therefore resolves against each model's own `file_path`.
    #[test]
    fn ownership_resolves_contract_stem_and_contract_sidecar() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let dir = TempDir::new().unwrap();
        // `orders.contract.sql` has stem `orders.contract`, so its ordinary
        // sidecar is `orders.contract.toml` — the same filename that would be
        // `orders.sql`'s contract. Name inference cannot separate them.
        write_model_with_identity(
            dir.path(),
            "orders.contract",
            "orders_contract",
            "orders_contract",
            "SELECT id FROM src_orders",
        );
        let compile = compile_head(dir.path(), sources).expect("compile");
        let contract_stem = compile
            .project
            .models
            .iter()
            .find(|model| model.config.name == "orders_contract")
            .expect("model with a .contract stem must load");

        assert!(model_owns_file(contract_stem, "orders.contract.sql"));
        assert!(model_owns_file(contract_stem, "orders.contract.toml"));
        assert!(!model_owns_file(contract_stem, "orders.sql"));
        assert!(!model_owns_file(contract_stem, "orders.toml"));

        // The sidecar change must reach the model rather than being rewritten
        // to the stem `orders`, which matches nothing.
        let changes = classify_model_changes(
            &[changed("models/orders.contract.toml", 'M')],
            Some("models"),
            Some(&compile.project.models),
            Some(&compile.project.models),
        );
        assert_eq!(changes.len(), 1, "got {changes:?}");
        assert_eq!(
            changes["c.s.orders_contract"].status(),
            ModelDiffStatus::Modified
        );
    }

    /// The compiler rejects duplicate `config.name` but not duplicate targets.
    /// Keying purely by target would let one model overwrite the other and
    /// vanish from the report.
    #[test]
    fn colliding_targets_keep_both_models() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let dir = TempDir::new().unwrap();
        for name in ["a", "b"] {
            write_model_with_identity(
                dir.path(),
                name,
                name,
                "shared",
                "SELECT id FROM src_orders",
            );
        }
        let compile = compile_head(dir.path(), sources).expect("compile");

        let changes = classify_model_changes(
            &[changed("models/a.sql", 'M'), changed("models/b.sql", 'M')],
            Some("models"),
            Some(&compile.project.models),
            Some(&compile.project.models),
        );

        assert_eq!(
            changes.len(),
            2,
            "neither model may be dropped: {changes:?}"
        );
        let names: Vec<&str> = changes
            .values()
            .map(|change| change.model_name.as_str())
            .collect();
        assert!(names.contains(&"a"), "got {names:?}");
        assert!(names.contains(&"b"), "got {names:?}");
    }

    /// A model whose target moves appears twice under one `model_name`
    /// (removed + added), so sorting on the name alone is not a total order —
    /// a stable sort over randomized `HashMap` iteration would leave the
    /// result vector, and the Markdown built from it, varying per process.
    #[test]
    fn build_results_order_is_total_for_duplicate_model_names() {
        // Every entry carries the SAME `model_name`, so sorting on the name
        // alone leaves the order entirely to `HashMap` iteration. Each map is
        // rebuilt from scratch so it gets a fresh `RandomState` — the same
        // thing that varies between processes — and the column counts make the
        // entries distinguishable in the assertion.
        // Every entry is named `orders`, so the projection below must key on
        // something *other* than the name or the assertion is vacuous: each
        // entry removes a distinct number of columns, making its position in
        // the output observable.
        let mut base_schemas: HashMap<String, Vec<TypedColumn>> = HashMap::new();
        for i in 0..8 {
            base_schemas.insert(
                format!("s{i}"),
                (0..=i)
                    .map(|c| TypedColumn {
                        name: format!("col{c}"),
                        data_type: "Int64".to_string(),
                    })
                    .collect(),
            );
        }
        let build_map = || {
            let mut changes: HashMap<String, ModelChange> = HashMap::new();
            for i in 0..8 {
                changes.insert(
                    format!("c.s{i}.orders"),
                    ModelChange {
                        model_name: "orders".to_string(),
                        base_schema_name: Some(format!("s{i}")),
                        head_schema_name: None,
                        resolved_target: None,
                        ..Default::default()
                    },
                );
            }
            changes
        };

        let order_of = |changes: &HashMap<String, ModelChange>| -> Vec<usize> {
            build_diff_results(changes, &HashMap::new(), &base_schemas)
                .into_iter()
                .map(|result| result.column_changes.len())
                .collect()
        };

        // Keys sort `c.s0` … `c.s7`, so the column counts come back 1..=8.
        let baseline = order_of(&build_map());
        assert_eq!(
            baseline,
            (1..=8).collect::<Vec<_>>(),
            "ties must resolve to change-map key order"
        );
        for _ in 0..64 {
            assert_eq!(
                order_of(&build_map()),
                baseline,
                "result order must not depend on HashMap iteration order"
            );
        }

        // And the tie-break is the change-map key, so the order is the keys'.
        let mut two: HashMap<String, ModelChange> = HashMap::new();
        two.insert(
            "c.s_old.orders".into(),
            model_change("orders", ModelDiffStatus::Removed),
        );
        two.insert(
            "c.s_new.orders".into(),
            model_change("orders", ModelDiffStatus::Added),
        );
        let results = build_diff_results(&two, &HashMap::new(), &HashMap::new());
        assert_eq!(results[0].status, ModelDiffStatus::Added, "c.s_new first");
        assert_eq!(results[1].status, ModelDiffStatus::Removed);
    }

    #[test]
    fn contract_sidecar_add_or_delete_keeps_existing_model_modified() {
        let dir = TempDir::new().unwrap();
        write_model(
            dir.path(),
            "orders",
            "SELECT id FROM (SELECT 1 AS id) AS src",
        );
        let compile = compile_head(dir.path(), HashMap::new()).expect("compile");

        for status in ['A', 'D'] {
            let files = vec![changed("models/orders.contract.toml", status)];
            let changes = classify_model_changes(
                &files,
                Some("models"),
                Some(&compile.project.models),
                Some(&compile.project.models),
            );
            assert_eq!(changes.len(), 1);
            assert_eq!(changes["c.s.orders"].status(), ModelDiffStatus::Modified);
        }
    }

    #[test]
    fn added_sql_does_not_match_existing_same_stem_rocky_model() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let base_dir = TempDir::new().unwrap();
        fs::write(
            base_dir.path().join("orders.rocky"),
            "from src_orders\nselect { id }\n",
        )
        .unwrap();
        let head_dir = TempDir::new().unwrap();
        fs::write(
            head_dir.path().join("orders.rocky"),
            "from src_orders\nselect { id }\n",
        )
        .unwrap();
        fs::write(
            head_dir.path().join("orders.sql"),
            "---toml\nname = \"orders_sql\"\ntarget = { catalog = \"c\", schema = \"s\", table = \"orders_sql\" }\n---\nSELECT id FROM src_orders\n",
        )
        .unwrap();
        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");

        let changes = classify_model_changes(
            &[changed("models/orders.sql", 'A')],
            Some("models"),
            Some(&base_compile.project.models),
            Some(&head_compile.project.models),
        );
        assert_eq!(changes.len(), 1);
        assert_eq!(changes["c.s.orders_sql"].status(), ModelDiffStatus::Added);
    }

    #[test]
    fn project_ir_from_compile_stitches_typed_columns_from_type_check() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path();
        // SELECT FROM a seeded source so the typechecker produces real
        // typed columns; SELECT-without-FROM yields an empty schema.
        write_model(models_dir, "orders", "SELECT id, name FROM src_orders");

        let sources = source_schema(
            "src_orders",
            &[
                ("id", rocky_ir::RockyType::Int64),
                ("name", rocky_ir::RockyType::String),
            ],
        );
        let result = compile_head(models_dir, sources).expect("compile succeeds");
        let ir = project_ir_from_compile(&result);

        assert_eq!(ir.models.len(), 1);
        let model = &ir.models[0];
        assert_eq!(&*model.name, "orders");
        assert!(
            !model.typed_columns.is_empty(),
            "typed_columns must be stitched from type_check.typed_models",
        );
        let names: Vec<&str> = model
            .typed_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
    }

    #[test]
    fn semantic_findings_flag_column_drop_as_breaking() {
        // Compile two minimal projects that differ only by a dropped
        // column on a shared model; assert the classifier surfaces a
        // `column_dropped` finding with `breaking` severity via the
        // stitched IR.
        let sources = source_schema(
            "src_orders",
            &[
                ("id", rocky_ir::RockyType::Int64),
                ("legacy_flag", rocky_ir::RockyType::String),
            ],
        );

        let base_dir = TempDir::new().unwrap();
        write_model(
            base_dir.path(),
            "orders",
            "SELECT id, legacy_flag FROM src_orders",
        );
        let head_dir = TempDir::new().unwrap();
        write_model(head_dir.path(), "orders", "SELECT id FROM src_orders");

        let base_compile = compile_head(base_dir.path(), sources.clone()).expect("base compile");
        let head_compile = compile_head(head_dir.path(), sources).expect("head compile");

        let findings = semantic_findings(Some(&base_compile), Some(&head_compile));
        let dropped: Vec<_> = findings
            .iter()
            .filter(|f| {
                matches!(
                    f.change,
                    rocky_core::breaking_change::BreakingChange::ColumnDropped { .. }
                )
            })
            .collect();
        assert_eq!(
            dropped.len(),
            1,
            "expected exactly one column_dropped finding, got findings: {findings:?}",
        );
        assert!(
            dropped[0].is_breaking(),
            "column_dropped must surface as breaking severity",
        );
    }

    #[test]
    fn semantic_findings_empty_when_either_side_missing() {
        // No compile on either side → classifier is skipped, empty vec.
        // The CLI relies on `skip_serializing_if = "Vec::is_empty"` to
        // omit the field from JSON output in this case.
        assert!(semantic_findings(None, None).is_empty());
    }

    // -----------------------------------------------------------------------
    // End-to-end: real git repository through `compute_ci_diff`
    //
    // The unit tests above hand `classify_model_changes` pre-parsed
    // `--name-status` bytes, which skips the whole git seam: rename detection,
    // repo-relative models-root discovery, base extraction, and the two gates
    // in `compute_ci_diff` that decide whether the compiler runs at all. These
    // drive the real thing.
    // -----------------------------------------------------------------------

    fn run_git(dir: &Path, args: &[&str]) {
        let out = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .expect("git command must run");
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Initialise a git repo with `models/`, commit `files`, and return the
    /// models directory. Model `name` and `target.table` are left to default
    /// from the filename stem — the shape #1225 is about.
    fn init_repo(dir: &Path, files: &[(&str, &str)]) -> std::path::PathBuf {
        let models_dir = dir.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            dir.join("rocky.toml"),
            "[adapter]\ntype = \"duckdb\"\npath = \":memory:\"\n",
        )
        .unwrap();
        run_git(dir, &["init", "-q", "-b", "main"]);
        run_git(dir, &["config", "user.email", "tester@example.com"]);
        run_git(dir, &["config", "user.name", "Tester"]);
        run_git(dir, &["config", "commit.gpgsign", "false"]);
        for (path, contents) in files {
            let full = models_dir.join(path);
            if let Some(parent) = full.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&full, contents).unwrap();
        }
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "base"]);
        models_dir
    }

    fn inferred_sidecar(schema: &str) -> String {
        format!(
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"poc\"\nschema = \"{schema}\"\n"
        )
    }

    /// Drive the diff against the scratch repository at `dir`.
    ///
    /// Roots git at `dir` explicitly rather than calling `set_current_dir`:
    /// cwd is process-wide, and under `cargo test` this crate's tests are
    /// threads in a single process, so mutating it races every other test that
    /// resolves a relative path.
    fn compute_in(dir: &Path, models_dir: &Path) -> CiDiffData {
        compute_ci_diff_in(
            &dir.join("rocky.toml"),
            &dir.join("state.redb"),
            "HEAD~1",
            models_dir,
            None,
            Some(dir),
        )
        .expect("compute_ci_diff must succeed")
    }

    /// The regression #1225 asked for: renaming a model whose `name` and
    /// `target.table` both default from the filename stem changes its resolved
    /// identity, so it is a removal plus an addition — not one empty
    /// modification. Driven through a real `git mv`.
    #[test]
    fn e2e_filename_inferred_rename_reports_removed_and_added() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = init_repo(
            dir,
            &[
                ("orders.sql", "SELECT 1 AS id, 'paid' AS status\n"),
                ("orders.toml", &inferred_sidecar("marts")),
                ("control.sql", "SELECT 1 AS id\n"),
                ("control.toml", &inferred_sidecar("marts")),
            ],
        );

        run_git(dir, &["mv", "models/orders.sql", "models/purchases.sql"]);
        run_git(dir, &["mv", "models/orders.toml", "models/purchases.toml"]);
        fs::write(
            models_dir.join("control.sql"),
            "SELECT 1 AS id, 2 AS extra\n",
        )
        .unwrap();
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "rename orders -> purchases"]);

        let data = compute_in(dir, &models_dir);

        let by_name: HashMap<&str, &DiffResult> = data
            .results
            .iter()
            .map(|r| (r.model_name.as_str(), r))
            .collect();
        assert_eq!(data.summary.removed, 1, "results: {:?}", data.results);
        assert_eq!(data.summary.added, 1, "results: {:?}", data.results);
        assert_eq!(by_name["orders"].status, ModelDiffStatus::Removed);
        assert_eq!(by_name["purchases"].status, ModelDiffStatus::Added);
        // The in-place edit must still be a plain modification alongside it.
        assert_eq!(by_name["control"].status, ModelDiffStatus::Modified);
        assert!(
            by_name["orders"]
                .column_changes
                .iter()
                .all(|c| c.change_type == ColumnChangeType::Removed),
            "removed model reports its columns as removed"
        );
        assert!(!by_name["orders"].column_changes.is_empty());
        assert!(!by_name["purchases"].column_changes.is_empty());
    }

    /// #1225's other half: when explicit `name` and `target` keep the resolved
    /// identity stable, a file-only rename stays one modification.
    #[test]
    fn e2e_rename_with_stable_identity_stays_modified() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let pinned = "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"poc\"\nschema = \"marts\"\ntable = \"orders\"\n";
        let models_dir = init_repo(
            dir,
            &[
                ("orders_v1.sql", "SELECT 1 AS id\n"),
                ("orders_v1.toml", pinned),
            ],
        );

        run_git(dir, &["mv", "models/orders_v1.sql", "models/orders_v2.sql"]);
        run_git(
            dir,
            &["mv", "models/orders_v1.toml", "models/orders_v2.toml"],
        );
        run_git(dir, &["commit", "-q", "-m", "rename files only"]);

        let data = compute_in(dir, &models_dir);
        assert_eq!(data.results.len(), 1, "results: {:?}", data.results);
        assert_eq!(data.results[0].model_name, "orders");
        assert_eq!(data.results[0].status, ModelDiffStatus::Modified);
    }

    /// A config group supplies `schema_template` to its members, so editing it
    /// retargets every member without touching a model file. That produces no
    /// structural rows — but it must still compile both refs, or the
    /// `--semantic` breaking-change classifier silently never runs.
    #[test]
    fn e2e_group_config_change_still_compiles_for_semantic() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = init_repo(
            dir,
            &[
                ("groups/emea.toml", "schema_template = \"mart_emea\"\n"),
                ("orders.sql", "SELECT 1 AS id\n"),
                (
                    "orders.toml",
                    "name = \"orders\"\ngroup = \"emea\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"poc\"\ntable = \"orders\"\n",
                ),
            ],
        );

        fs::write(
            models_dir.join("groups/emea.toml"),
            "schema_template = \"mart_emea_v2\"\n",
        )
        .unwrap();
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "retarget the emea group"]);

        let data = compute_in(dir, &models_dir);

        // No model *file* changed, but a row identifies a target and the group
        // moved this model's: the old table is abandoned, a new one appears.
        let row = |status: ModelDiffStatus| {
            data.results
                .iter()
                .find(|result| result.status == status)
                .unwrap_or_else(|| panic!("no {status} row in {:?}", data.results))
        };
        assert_eq!(data.results.len(), 2, "results: {:?}", data.results);
        assert_eq!(
            row(ModelDiffStatus::Removed).resolved_target.as_deref(),
            Some("poc.mart_emea.orders")
        );
        assert_eq!(
            row(ModelDiffStatus::Added).resolved_target.as_deref(),
            Some("poc.mart_emea_v2.orders")
        );
        // Both rows name the same model, so the target is the only thing
        // telling them apart — the reason it is on `DiffResult` at all.
        assert_eq!(row(ModelDiffStatus::Removed).model_name, "orders");
        assert_eq!(row(ModelDiffStatus::Added).model_name, "orders");

        // The semantic gate must still fire alongside it.
        assert!(
            data.base_compile.is_some() && data.head_compile.is_some(),
            "shared-config change must not suppress the compiles"
        );
        let findings = semantic_findings(data.base_compile.as_ref(), data.head_compile.as_ref());
        assert!(
            findings
                .iter()
                .any(rocky_core::breaking_change::BreakingFinding::is_breaking),
            "retargeting a group must surface a breaking finding, got: {findings:?}"
        );
    }

    /// `_defaults.toml` supplies `target.schema` to every model in the
    /// directory, so editing it moves all of them. Same shape as the group
    /// case, different shared-config file.
    #[test]
    fn e2e_defaults_retarget_reports_both_targets() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = init_repo(
            dir,
            &[
                (
                    "_defaults.toml",
                    "[target]\ncatalog = \"poc\"\nschema = \"marts\"\n",
                ),
                ("orders.sql", "SELECT 1 AS id\n"),
                (
                    "orders.toml",
                    "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n",
                ),
            ],
        );

        fs::write(
            models_dir.join("_defaults.toml"),
            "[target]\ncatalog = \"poc\"\nschema = \"marts_v2\"\n",
        )
        .unwrap();
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "retarget via _defaults"]);

        let data = compute_in(dir, &models_dir);
        let target = |status: ModelDiffStatus| {
            data.results
                .iter()
                .find(|r| r.status == status)
                .and_then(|r| r.resolved_target.as_deref())
        };
        assert_eq!(data.results.len(), 2, "results: {:?}", data.results);
        assert_eq!(target(ModelDiffStatus::Removed), Some("poc.marts.orders"));
        assert_eq!(target(ModelDiffStatus::Added), Some("poc.marts_v2.orders"));
    }

    /// A target that could never name a warehouse object must not be published
    /// as one. Components are not validated at load — only at SQL generation —
    /// so `ci-diff` has to check before it publishes.
    #[test]
    fn unvalidatable_target_is_not_published_as_an_identity() {
        let sources = source_schema("src_orders", &[("id", rocky_ir::RockyType::Int64)]);
        let dir = TempDir::new().unwrap();
        write_model_with_identity(
            dir.path(),
            "orders",
            "orders",
            "not a valid identifier",
            "SELECT id FROM src_orders",
        );
        let compile = compile_head(dir.path(), sources).expect("compile");
        let model = &compile.project.models[0];
        assert!(
            publishable_target(model).is_none(),
            "an invalid target must not be published, got {:?}",
            publishable_target(model)
        );

        // A well-formed one still is.
        let ok_dir = TempDir::new().unwrap();
        write_model_with_identity(
            ok_dir.path(),
            "orders",
            "orders",
            "orders",
            "SELECT 1 AS id",
        );
        let ok = compile_head(ok_dir.path(), HashMap::new()).expect("compile");
        assert_eq!(
            publishable_target(&ok.project.models[0]).as_deref(),
            Some("c.s.orders")
        );
    }

    /// A shared-config edit that moves nothing must stay quiet — the scan is
    /// proportional to the retargeted set, not to the project.
    #[test]
    fn e2e_shared_config_edit_that_moves_nothing_reports_nothing() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = init_repo(
            dir,
            &[
                (
                    "_defaults.toml",
                    "[target]\ncatalog = \"poc\"\nschema = \"marts\"\n",
                ),
                ("orders.sql", "SELECT 1 AS id\n"),
                (
                    "orders.toml",
                    "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n",
                ),
            ],
        );

        // Touch the shared config without changing any resolved target.
        fs::write(
            models_dir.join("_defaults.toml"),
            "# routing for the marts layer\n[target]\ncatalog = \"poc\"\nschema = \"marts\"\n",
        )
        .unwrap();
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "comment only"]);

        let data = compute_in(dir, &models_dir);
        assert!(
            data.results.is_empty(),
            "a no-op shared-config edit must not emit rows: {:?}",
            data.results
        );
    }

    /// The converse: a change that touches nothing under the models root must
    /// still short-circuit before the compiler runs.
    #[test]
    fn e2e_non_model_change_skips_compilation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = init_repo(
            dir,
            &[
                ("orders.sql", "SELECT 1 AS id\n"),
                ("orders.toml", &inferred_sidecar("marts")),
            ],
        );

        fs::write(dir.join("README.md"), "docs\n").unwrap();
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "docs only"]);

        let data = compute_in(dir, &models_dir);
        assert!(data.results.is_empty());
        assert_eq!(data.changed_file_count, 1);
        assert!(
            data.base_compile.is_none() && data.head_compile.is_none(),
            "a docs-only change must not pay for two compiles"
        );
    }

    /// A non-compiler file *inside* the models root must not buy two compiles
    /// either — only shapes the loaders can actually read do.
    #[test]
    fn e2e_non_compiler_file_inside_models_root_skips_compilation() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = init_repo(
            dir,
            &[
                ("orders.sql", "SELECT 1 AS id\n"),
                ("orders.toml", &inferred_sidecar("marts")),
                ("README.md", "notes\n"),
            ],
        );

        fs::write(models_dir.join("README.md"), "more notes\n").unwrap();
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "models/README.md only"]);

        let data = compute_in(dir, &models_dir);
        assert!(data.results.is_empty());
        assert!(
            data.base_compile.is_none() && data.head_compile.is_none(),
            "a doc file under models/ cannot change a compile"
        );
    }

    /// A model whose stem is `rocky` is an ordinary model — the reserved-name
    /// exclusion applies to shared TOML config only.
    #[test]
    fn e2e_reserved_stem_sql_file_is_a_real_model() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = init_repo(
            dir,
            &[
                ("rocky.sql", "SELECT 1 AS id\n"),
                ("rocky.toml", &inferred_sidecar("marts")),
            ],
        );

        fs::write(models_dir.join("rocky.sql"), "SELECT 1 AS id, 2 AS extra\n").unwrap();
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "add a column"]);

        let data = compute_in(dir, &models_dir);
        assert_eq!(data.results.len(), 1, "results: {:?}", data.results);
        assert_eq!(data.results[0].model_name, "rocky");
        assert_eq!(data.results[0].status, ModelDiffStatus::Modified);
    }
}
