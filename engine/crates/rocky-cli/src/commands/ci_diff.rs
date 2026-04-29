//! `rocky ci-diff` — detect changed models between git refs and generate a structural diff report.
//!
//! Shells out to `git diff --name-only` to find `.sql`, `.rocky`, and `.toml`
//! sidecar files that changed between a base ref (default: `main`) and HEAD.
//! Compiles the current working tree to extract model schemas, then classifies
//! each changed model as added, modified, or removed and generates a structured
//! diff report in JSON and Markdown formats.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use tracing::debug;

use rocky_compiler::compile::{self, CompilerConfig};
use rocky_core::ci_diff::{
    ColumnChangeType, ColumnDiff, DiffResult, DiffSummary, ModelDiffStatus, format_diff_markdown,
    format_diff_table,
};

use crate::output::{CiDiffOutput, print_json};

// ---------------------------------------------------------------------------
// Git integration
// ---------------------------------------------------------------------------

/// A file change detected by git between two refs.
#[derive(Debug, Clone)]
struct ChangedFile {
    /// Path relative to the repository root.
    path: String,
    /// Git diff status: A (added), D (deleted), M (modified), R (renamed), etc.
    status: char,
}

/// Run `git diff --name-status` between `base_ref` and HEAD to find changed files.
///
/// Uses three-dot syntax (`base...HEAD`) for merge-base semantics — this matches
/// what CI systems care about: changes since the branch diverged from the base,
/// not changes since the base's current tip.
fn git_changed_files(base_ref: &str) -> Result<Vec<ChangedFile>> {
    let output = Command::new("git")
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
        let output = Command::new("git")
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
        let path = parts.next().unwrap_or("");

        // For renames (R100), use the new path
        let effective_path = parts.next().unwrap_or(path);
        let status = status_str.chars().next().unwrap_or('M');

        if !effective_path.is_empty() {
            files.push(ChangedFile {
                path: effective_path.to_string(),
                status,
            });
        }
    }

    Ok(files)
}

/// Filter changed files to model files (.sql, .rocky) and their sidecars (.toml).
///
/// Returns a map from model stem (filename without extension) to its change status.
fn classify_model_changes(files: &[ChangedFile]) -> HashMap<String, ModelDiffStatus> {
    let mut models: HashMap<String, ModelDiffStatus> = HashMap::new();

    for file in files {
        let path = Path::new(&file.path);
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

        // Only care about model files and their sidecars
        if ext != "sql" && ext != "rocky" && ext != "toml" {
            continue;
        }

        // Skip non-model TOML files (e.g., rocky.toml, Cargo.toml)
        // Model sidecars live next to .sql/.rocky files in a models/ directory
        if ext == "toml" {
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            // _defaults.toml is a directory-level config, not a model sidecar
            if stem == "_defaults" || stem == "rocky" || stem == "Cargo" {
                continue;
            }
        }

        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();

        if stem.is_empty() {
            continue;
        }

        let status = match file.status {
            'A' => ModelDiffStatus::Added,
            'D' => ModelDiffStatus::Removed,
            _ => ModelDiffStatus::Modified,
        };

        // If we already classified this model (e.g. both .sql and .toml changed),
        // prefer the more significant status
        models
            .entry(stem)
            .and_modify(|existing| {
                // Added/Removed take priority over Modified
                if *existing == ModelDiffStatus::Modified {
                    *existing = status;
                }
            })
            .or_insert(status);
    }

    models
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

/// Try to extract schemas from the base ref by checking out model files to a temp dir.
///
/// This is best-effort: if the base ref doesn't have a complete models directory
/// or compilation fails, we return an empty map rather than erroring.
///
/// Arc 7 wave 2 wave-2 note: `source_schemas` seeds the compile from the
/// *current* warehouse cache, not historical types. That's fine for diff
/// purposes — typecheck on historical models with today's leaf types still
/// detects the model-level schema drift that ci-diff is looking for, and
/// there's no per-ref cache to restore from.
fn extract_base_schemas(
    base_ref: &str,
    models_dir: &Path,
    source_schemas: HashMap<String, Vec<rocky_compiler::types::TypedColumn>>,
) -> HashMap<String, Vec<TypedColumn>> {
    // Try to get the models directory path relative to the repo root
    let models_rel = find_models_relative_path(models_dir);
    let models_rel = match models_rel {
        Some(p) => p,
        None => {
            debug!("could not determine models directory relative to repo root");
            return HashMap::new();
        }
    };

    // Create a temp directory and populate it with base-ref model files
    let tmp = match tempfile::tempdir() {
        Ok(t) => t,
        Err(e) => {
            debug!("failed to create temp dir for base schema extraction: {e}");
            return HashMap::new();
        }
    };

    // Use git ls-tree to find all files under the models path at the base ref
    let ls_output = Command::new("git")
        .args(["ls-tree", "-r", "--name-only", base_ref, &models_rel])
        .output();

    let ls_output = match ls_output {
        Ok(o) if o.status.success() => o,
        _ => {
            debug!(
                "git ls-tree failed for base ref '{base_ref}' — skipping base schema extraction"
            );
            return HashMap::new();
        }
    };

    let file_list = String::from_utf8_lossy(&ls_output.stdout);
    for file_path in file_list.lines() {
        let file_path = file_path.trim();
        if file_path.is_empty() {
            continue;
        }

        // Determine the relative path within the models directory
        let rel = match file_path.strip_prefix(&models_rel) {
            Some(r) => r.trim_start_matches('/'),
            None => continue,
        };

        let dest = tmp.path().join(rel);
        if let Some(parent) = dest.parent() {
            let _ = std::fs::create_dir_all(parent);
        }

        // Extract file content from git
        let show_output = Command::new("git")
            .args(["show", &format!("{base_ref}:{file_path}")])
            .output();

        if let Ok(o) = show_output {
            if o.status.success() {
                let _ = std::fs::write(&dest, &o.stdout);
            }
        }
    }

    // Try to compile the extracted models
    let config = CompilerConfig {
        models_dir: tmp.path().to_path_buf(),
        contracts_dir: None,
        source_schemas,
        source_column_info: HashMap::new(),
        ..Default::default()
    };

    match compile::compile(&config) {
        Ok(result) => {
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
        Err(e) => {
            debug!("base ref compilation failed (expected for partial models): {e}");
            HashMap::new()
        }
    }
}

/// Find the models directory path relative to the git repo root.
fn find_models_relative_path(models_dir: &Path) -> Option<String> {
    let abs_models = std::fs::canonicalize(models_dir).ok()?;

    let output = Command::new("git")
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
    model_changes: &HashMap<String, ModelDiffStatus>,
    head_schemas: &HashMap<String, Vec<TypedColumn>>,
    base_schemas: &HashMap<String, Vec<TypedColumn>>,
) -> Vec<DiffResult> {
    let mut results: Vec<DiffResult> = model_changes
        .iter()
        .map(|(name, status)| {
            let column_changes = match status {
                ModelDiffStatus::Modified => {
                    let base_cols = base_schemas.get(name);
                    let head_cols = head_schemas.get(name);
                    match (base_cols, head_cols) {
                        (Some(base), Some(head)) => diff_columns(base, head),
                        _ => vec![],
                    }
                }
                ModelDiffStatus::Added => {
                    // Show all columns as added for new models
                    head_schemas
                        .get(name)
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
                    base_schemas
                        .get(name)
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

            DiffResult {
                model_name: name.clone(),
                status: *status,
                row_count_before: None,
                row_count_after: None,
                column_changes,
                sample_changed_rows: None,
            }
        })
        .collect();

    // Sort by model name for deterministic output
    results.sort_by(|a, b| a.model_name.cmp(&b.model_name));
    results
}

// ---------------------------------------------------------------------------
// Shared diff computation
// ---------------------------------------------------------------------------

/// Result of computing a CI diff.
///
/// `head_compile` is `None` when the models directory is missing or the
/// HEAD compile fails — callers (e.g. `rocky lineage-diff`) that need the
/// `semantic_graph` for downstream traces must handle that gracefully.
pub(crate) struct CiDiffData {
    pub(crate) summary: DiffSummary,
    pub(crate) results: Vec<DiffResult>,
    pub(crate) head_compile: Option<rocky_compiler::compile::CompileResult>,
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
    // Wave-2 of Arc 7 wave 2: load cached source schemas once and seed
    // both compiles (current tree + base ref) with the same map so the
    // resulting per-model type diffs measure real schema drift rather
    // than `Unknown`-vs-`Unknown` noise. Degrades to empty when the
    // cache is cold or `[cache.schemas] enabled = false`.
    let source_schemas = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(cache_ttl_override);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, state_path)
        }
        Err(_) => HashMap::new(),
    };

    let changed_files = git_changed_files(base_ref)?;
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
            changed_file_count,
        });
    }

    let model_changes = classify_model_changes(&changed_files);
    if model_changes.is_empty() {
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

    let base_schemas = if models_dir.is_dir() {
        extract_base_schemas(base_ref, models_dir, source_schemas)
    } else {
        HashMap::new()
    };

    let results = build_diff_results(&model_changes, &head_schemas, &base_schemas);
    let summary = DiffSummary::from_results(&results);

    Ok(CiDiffData {
        summary,
        results,
        head_compile,
        changed_file_count,
    })
}

// ---------------------------------------------------------------------------
// Public command entry point
// ---------------------------------------------------------------------------

/// Execute `rocky ci-diff`.
pub fn run_ci_diff(
    config_path: &Path,
    state_path: &Path,
    base_ref: &str,
    models_dir: &Path,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let data = compute_ci_diff(
        config_path,
        state_path,
        base_ref,
        models_dir,
        cache_ttl_override,
    )?;

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
            );
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
        }
        return Ok(());
    }

    if output_json {
        let output = CiDiffOutput::new(
            base_ref.to_string(),
            "HEAD".to_string(),
            data.summary,
            data.results,
        );
        print_json(&output)?;
    } else {
        println!("Rocky CI Diff ({base_ref}...HEAD)\n");
        print!("{}", format_diff_table(&data.results));
        println!();
        println!("--- Markdown (for PR comment) ---\n");
        print!("{}", format_diff_markdown(&data.results));
    }

    Ok(())
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

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
    fn parse_rename_uses_new_path() {
        let raw = b"R100\tmodels/old_name.sql\tmodels/new_name.sql\n";
        let files = parse_name_status(raw).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].status, 'R');
        assert_eq!(files[0].path, "models/new_name.sql");
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
            ChangedFile {
                path: "models/orders.sql".into(),
                status: 'A',
            },
            ChangedFile {
                path: "models/customers.sql".into(),
                status: 'M',
            },
            ChangedFile {
                path: "models/legacy.sql".into(),
                status: 'D',
            },
        ];
        let changes = classify_model_changes(&files);
        assert_eq!(changes.get("orders"), Some(&ModelDiffStatus::Added));
        assert_eq!(changes.get("customers"), Some(&ModelDiffStatus::Modified));
        assert_eq!(changes.get("legacy"), Some(&ModelDiffStatus::Removed));
    }

    #[test]
    fn classify_rocky_files() {
        let files = vec![ChangedFile {
            path: "models/pipeline.rocky".into(),
            status: 'A',
        }];
        let changes = classify_model_changes(&files);
        assert_eq!(changes.get("pipeline"), Some(&ModelDiffStatus::Added));
    }

    #[test]
    fn classify_toml_sidecars() {
        let files = vec![ChangedFile {
            path: "models/orders.toml".into(),
            status: 'M',
        }];
        let changes = classify_model_changes(&files);
        assert_eq!(changes.get("orders"), Some(&ModelDiffStatus::Modified));
    }

    #[test]
    fn classify_ignores_non_model_files() {
        let files = vec![
            ChangedFile {
                path: "rocky.toml".into(),
                status: 'M',
            },
            ChangedFile {
                path: "Cargo.toml".into(),
                status: 'M',
            },
            ChangedFile {
                path: "models/_defaults.toml".into(),
                status: 'M',
            },
            ChangedFile {
                path: "README.md".into(),
                status: 'M',
            },
            ChangedFile {
                path: "src/main.rs".into(),
                status: 'M',
            },
        ];
        let changes = classify_model_changes(&files);
        assert!(changes.is_empty());
    }

    #[test]
    fn classify_combined_sql_and_toml_prefers_significant() {
        // When both .sql (Added) and .toml (Modified) change for the same model,
        // the more significant status (Added) should win.
        let files = vec![
            ChangedFile {
                path: "models/orders.toml".into(),
                status: 'M',
            },
            ChangedFile {
                path: "models/orders.sql".into(),
                status: 'A',
            },
        ];
        let changes = classify_model_changes(&files);
        assert_eq!(changes.get("orders"), Some(&ModelDiffStatus::Added));
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
        model_changes.insert("zebra".into(), ModelDiffStatus::Added);
        model_changes.insert("alpha".into(), ModelDiffStatus::Modified);

        let results = build_diff_results(&model_changes, &HashMap::new(), &HashMap::new());
        assert_eq!(results[0].model_name, "alpha");
        assert_eq!(results[1].model_name, "zebra");
    }

    #[test]
    fn build_results_with_schemas() {
        let mut model_changes = HashMap::new();
        model_changes.insert("orders".into(), ModelDiffStatus::Modified);

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
        model_changes.insert("new_model".into(), ModelDiffStatus::Added);

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
        model_changes.insert("old_model".into(), ModelDiffStatus::Removed);

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
}
