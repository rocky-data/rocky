//! `rocky adapter` — discover and inspect process adapters on `$PATH`.
//!
//! Process adapters are external binaries that implement the Rocky JSON-RPC
//! adapter protocol over stdio (see [`rocky_adapter_sdk::process`]). Rocky
//! follows the `cargo`-subcommand convention: any executable on `$PATH` whose
//! filename starts with `rocky-` and isn't a known Rocky subcommand binary
//! is treated as an installable process adapter named after the suffix.
//!
//! # Discovery
//!
//! `rocky adapter list` walks the `PATH` directories and emits one entry per
//! `rocky-<name>` executable found. Adapters that fail to spawn or fail to
//! return a manifest on `initialize` still appear in the listing with an
//! error message — the goal is "tell the user what would happen if they
//! pointed Rocky at this adapter", not to silently hide broken installs.
//!
//! # Inspection
//!
//! `rocky adapter info <name>` spawns the adapter, performs an `initialize`
//! handshake, and prints the manifest (name, version, SDK version, dialect,
//! capabilities, auth methods, config schema).

use std::collections::BTreeMap;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rocky_adapter_sdk::WarehouseAdapter;
use rocky_adapter_sdk::manifest::AdapterManifest;
use rocky_adapter_sdk::process::ProcessAdapter;
use serde::Serialize;

/// File-name prefix Rocky treats as a process adapter.
pub const ADAPTER_PREFIX: &str = "rocky-";

/// File-name suffixes that look like `rocky-*` binaries but are first-party
/// Rocky tools (or shipped alongside the engine), not process adapters.
///
/// `rocky-lsp` is the language server bundled with the engine release; `rocky`
/// itself is the CLI. Anything else on `PATH` with the `rocky-` prefix is
/// assumed to be a process adapter someone installed.
const BUILTIN_BIN_SUFFIXES: &[&str] = &["lsp"];

/// One entry in `rocky adapter list` output.
#[derive(Debug, Clone, Serialize)]
pub struct AdapterListEntry {
    /// Adapter name as derived from the executable filename
    /// (`rocky-foo` -> `"foo"`).
    pub name: String,
    /// Absolute path to the executable.
    pub path: String,
    /// Manifest if `initialize` succeeded, `None` otherwise.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<AdapterManifest>,
    /// Spawn or initialize error message, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Resolve an adapter name (e.g. `"foo"`) to a `rocky-foo` binary path on `$PATH`.
///
/// Returns `None` if no matching executable is found.
pub fn resolve_adapter_command(name: &str) -> Option<PathBuf> {
    let bin = format!("{ADAPTER_PREFIX}{name}");
    which_in_path(&bin)
}

/// Walk `$PATH` and return every `rocky-<name>` executable, deduplicated by
/// adapter name (first hit on `$PATH` wins, mirroring shell behaviour).
pub fn discover_adapters_on_path() -> BTreeMap<String, PathBuf> {
    let mut found: BTreeMap<String, PathBuf> = BTreeMap::new();
    let Some(path_var) = env::var_os("PATH") else {
        return found;
    };

    for dir in env::split_paths(&path_var) {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str() else {
                continue;
            };
            // Strip a single trailing ".exe" so Windows binaries register
            // under the same logical name as Unix ones.
            let stem = name.strip_suffix(".exe").unwrap_or(name);
            let Some(suffix) = stem.strip_prefix(ADAPTER_PREFIX) else {
                continue;
            };
            if suffix.is_empty() || BUILTIN_BIN_SUFFIXES.contains(&suffix) {
                continue;
            }
            if !is_executable(&entry.path()) {
                continue;
            }
            // First hit on $PATH wins — matches shell PATH lookup semantics.
            found
                .entry(suffix.to_string())
                .or_insert_with(|| entry.path());
        }
    }

    found
}

/// Execute `rocky adapter list`.
pub async fn run_adapter_list(json: bool) -> Result<()> {
    let discovered = discover_adapters_on_path();
    let mut entries: Vec<AdapterListEntry> = Vec::with_capacity(discovered.len());

    for (name, path) in discovered {
        let path_str = path.display().to_string();
        match ProcessAdapter::spawn(&path_str, &[], &serde_json::json!({})).await {
            Ok(adapter) => {
                let manifest = adapter.manifest().clone();
                let _ = adapter.close().await;
                entries.push(AdapterListEntry {
                    name,
                    path: path_str,
                    manifest: Some(manifest),
                    error: None,
                });
            }
            Err(e) => {
                entries.push(AdapterListEntry {
                    name,
                    path: path_str,
                    manifest: None,
                    error: Some(e.to_string()),
                });
            }
        }
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&entries)
                .context("failed to serialize adapter list output")?
        );
    } else if entries.is_empty() {
        println!(
            "No process adapters found on $PATH. Install a `rocky-<name>` binary \
             on $PATH to register it."
        );
    } else {
        println!("{:<20} {:<12} {:<14} PATH", "NAME", "VERSION", "DIALECT");
        for e in &entries {
            let (version, dialect) = e
                .manifest
                .as_ref()
                .map(|m| (m.version.as_str(), m.dialect.as_str()))
                .unwrap_or(("error", "-"));
            println!("{:<20} {:<12} {:<14} {}", e.name, version, dialect, e.path);
        }
        for e in entries.iter().filter(|e| e.error.is_some()) {
            eprintln!(
                "warning: {} failed to initialize: {}",
                e.name,
                e.error.as_deref().unwrap_or("(unknown)")
            );
        }
    }

    Ok(())
}

/// Execute `rocky adapter info <name>`.
pub async fn run_adapter_info(name: &str, json: bool) -> Result<()> {
    let command = resolve_adapter_command(name).with_context(|| {
        format!(
            "no `{ADAPTER_PREFIX}{name}` binary found on $PATH. \
             Run `rocky adapter list` to see what's installed."
        )
    })?;
    let command_str = command.display().to_string();
    let adapter = ProcessAdapter::spawn(&command_str, &[], &serde_json::json!({}))
        .await
        .with_context(|| format!("failed to spawn adapter `{name}` at `{command_str}`"))?;
    let manifest = adapter.manifest().clone();
    let _ = adapter.close().await;

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&manifest)
                .context("failed to serialize adapter manifest")?
        );
    } else {
        println!("name:          {}", manifest.name);
        println!("path:          {command_str}");
        println!("version:       {}", manifest.version);
        println!("sdk_version:   {}", manifest.sdk_version);
        println!("dialect:       {}", manifest.dialect);
        println!("auth_methods:  {}", manifest.auth_methods.join(", "));
        println!("capabilities:");
        let c = &manifest.capabilities;
        println!("  warehouse       = {}", c.warehouse);
        println!("  discovery       = {}", c.discovery);
        println!("  governance      = {}", c.governance);
        println!("  batch_checks    = {}", c.batch_checks);
        println!("  create_catalog  = {}", c.create_catalog);
        println!("  create_schema   = {}", c.create_schema);
        println!("  merge           = {}", c.merge);
        println!("  tablesample     = {}", c.tablesample);
        println!("  file_load       = {}", c.file_load);
    }

    Ok(())
}

/// Tiny `which` shim: look up an executable across `$PATH` without taking
/// a new dependency. Returns the first match, mirroring shell semantics.
fn which_in_path(bin: &str) -> Option<PathBuf> {
    let path_var = env::var_os("PATH")?;
    for dir in env::split_paths(&path_var) {
        for candidate in candidate_names(bin) {
            let full = dir.join(&candidate);
            if is_executable(&full) {
                return Some(full);
            }
        }
    }
    None
}

/// On Windows, also try the `.exe` extension; on Unix the base name is enough.
fn candidate_names(bin: &str) -> Vec<String> {
    #[cfg(windows)]
    {
        // Honour PATHEXT for completeness; default to `.exe` if it's unset.
        let pathext = env::var("PATHEXT").unwrap_or_else(|_| ".EXE".into());
        let mut names = vec![bin.to_string()];
        for ext in pathext.split(';').filter(|e| !e.is_empty()) {
            names.push(format!("{bin}{}", ext.to_lowercase()));
        }
        names
    }
    #[cfg(not(windows))]
    {
        vec![bin.to_string()]
    }
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    meta.is_file() && (meta.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.is_file())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_names_unix_only_returns_one() {
        #[cfg(not(windows))]
        {
            let names = candidate_names("rocky-foo");
            assert_eq!(names, vec!["rocky-foo".to_string()]);
        }
    }

    #[test]
    fn discover_with_empty_path_returns_empty() {
        // SAFETY: tests run single-threaded for env var mutation in this
        // crate's existing pattern. We restore PATH on exit.
        let prev = env::var_os("PATH");
        // SAFETY: env mutation in a single-threaded test; restored before return.
        unsafe {
            env::set_var("PATH", "");
        }
        let found = discover_adapters_on_path();
        // SAFETY: restore the original PATH for subsequent tests.
        unsafe {
            match prev {
                Some(v) => env::set_var("PATH", v),
                None => env::remove_var("PATH"),
            }
        }
        assert!(found.is_empty(), "empty PATH should produce no adapters");
    }

    #[test]
    fn builtin_suffixes_are_filtered() {
        // The filter is on the file-name suffix, not the full path; we
        // exercise the constant here so a future addition can't silently
        // re-register `rocky-lsp` as a process adapter.
        assert!(BUILTIN_BIN_SUFFIXES.contains(&"lsp"));
    }
}
