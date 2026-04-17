//! Validates that every `rocky.toml` under `examples/playground/pocs/`
//! parses cleanly against the schemars-generated [`RockyConfig`] and
//! [`AdapterConfig`] schemas.
//!
//! Two complementary checks:
//!
//! 1. `every_committed_poc_adapter_block_matches_generated_schema` — focused
//!    check on `[adapter.*]` blocks. Predates the project-level schema and
//!    catches any drift in the [`AdapterConfig`] struct shape.
//!
//! 2. `every_committed_poc_matches_project_schema` — validates the full
//!    `rocky.toml` against the [`RockyConfig`] schema (top-level state,
//!    cost, hooks, schema_evolution, adapters). Pipelines pass through the
//!    permissive PR-a placeholder; PR-b adds the per-variant schema.
//!
//! When either fails, either the config-struct shape drifted from reality
//! or a POC introduced a shape the struct doesn't cover — both are real
//! problems to investigate before shipping.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use jsonschema::Validator;
use schemars::schema_for;

use rocky_core::config::{AdapterConfig, RockyConfig};

/// Resolve the repo-root `examples/playground/pocs/` directory, or return
/// `None` when the crate is checked out in isolation (e.g. packaged into
/// a docs build). Tests that depend on the pocs tree skip themselves in
/// that case rather than failing spuriously.
fn pocs_root() -> Option<PathBuf> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // crates/rocky-core → engine → monorepo root → examples/playground/pocs
    let path = Path::new(manifest_dir)
        .join("..")
        .join("..")
        .join("..")
        .join("examples")
        .join("playground")
        .join("pocs");
    if path.is_dir() { Some(path) } else { None }
}

fn walk_rocky_tomls(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name() == Some(std::ffi::OsStr::new("rocky.toml")) {
                out.push(path);
            }
        }
    }
    out.sort();
    out
}

/// Extract every `[adapter.*]` block (and the unnamed `[adapter]` block,
/// which Rocky auto-wraps as `adapter.default`) from a parsed
/// `rocky.toml`. Returns `(path, value)` tuples where `value` is the raw
/// TOML table for the adapter, serialized to JSON for validator input.
fn extract_adapter_blocks(
    toml_path: &Path,
) -> Result<Vec<(String, serde_json::Value)>, Box<dyn std::error::Error>> {
    let raw = std::fs::read_to_string(toml_path)?;
    let doc: toml::Value = toml::from_str(&raw)?;
    let table = doc.as_table().ok_or("rocky.toml root is not a table")?;

    let mut out = Vec::new();

    // Unnamed `[adapter]` with a `type` key is the canonical shape —
    // auto-wraps as `adapter.default`.
    if let Some(v) = table.get("adapter") {
        if let Some(t) = v.as_table() {
            if t.contains_key("type") {
                // Flat shape: promote to adapter.default.
                out.push(("adapter.default".to_owned(), serialize_toml(v)?));
            } else {
                // Nested shape: every sub-table is a named adapter.
                for (name, inner) in t {
                    if inner.is_table() {
                        out.push((format!("adapter.{name}"), serialize_toml(inner)?));
                    }
                }
            }
        }
    }

    // The `adapters` alias lives only as a nested table.
    if let Some(v) = table.get("adapters") {
        if let Some(t) = v.as_table() {
            for (name, inner) in t {
                if inner.is_table() {
                    out.push((format!("adapters.{name}"), serialize_toml(inner)?));
                }
            }
        }
    }

    Ok(out)
}

fn serialize_toml(v: &toml::Value) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    // toml::Value → serde_json::Value via the serde bridge.
    let json = serde_json::to_value(v)?;
    Ok(json)
}

#[test]
fn every_committed_poc_adapter_block_matches_generated_schema() {
    let Some(root) = pocs_root() else {
        eprintln!(
            "skipping every_committed_poc_adapter_block_matches_generated_schema: \
             examples/playground/pocs not reachable from CARGO_MANIFEST_DIR"
        );
        return;
    };

    let schema_value = serde_json::to_value(schema_for!(AdapterConfig))
        .expect("schema serialization is infallible");
    let validator = Validator::new(&schema_value).expect("generated schema is valid JSON Schema");

    let mut failures: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut total_blocks = 0usize;

    for toml_path in walk_rocky_tomls(&root) {
        let rel = toml_path
            .strip_prefix(&root)
            .unwrap_or(&toml_path)
            .display()
            .to_string();

        let blocks = match extract_adapter_blocks(&toml_path) {
            Ok(blocks) => blocks,
            Err(e) => {
                failures
                    .entry(rel)
                    .or_default()
                    .push(format!("failed to parse adapter blocks: {e}"));
                continue;
            }
        };

        for (section, value) in blocks {
            total_blocks += 1;
            let errs: Vec<String> = validator
                .iter_errors(&value)
                .map(|e| format!("  {section}: {e} at {}", e.instance_path))
                .collect();
            if !errs.is_empty() {
                failures.entry(rel.clone()).or_default().extend(errs);
            }
        }
    }

    assert!(
        total_blocks > 0,
        "expected at least one adapter block under {}; found none — did the walker miss the tree?",
        root.display()
    );

    assert!(
        failures.is_empty(),
        "generated AdapterConfig schema rejects committed POC adapter blocks:\n{}",
        failures
            .iter()
            .map(|(file, errs)| format!("- {file}\n{}", errs.join("\n")))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn every_committed_poc_matches_project_schema() {
    let Some(root) = pocs_root() else {
        eprintln!(
            "skipping every_committed_poc_matches_project_schema: \
             examples/playground/pocs not reachable from CARGO_MANIFEST_DIR"
        );
        return;
    };

    let schema_value =
        serde_json::to_value(schema_for!(RockyConfig)).expect("schema serialization is infallible");
    let validator = Validator::new(&schema_value).expect("generated schema is valid JSON Schema");

    let mut failures: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut total = 0usize;

    for toml_path in walk_rocky_tomls(&root) {
        total += 1;
        let rel = toml_path
            .strip_prefix(&root)
            .unwrap_or(&toml_path)
            .display()
            .to_string();

        let raw = match std::fs::read_to_string(&toml_path) {
            Ok(s) => s,
            Err(e) => {
                failures.entry(rel).or_default().push(format!("read: {e}"));
                continue;
            }
        };
        let doc: toml::Value = match toml::from_str(&raw) {
            Ok(v) => v,
            Err(e) => {
                failures.entry(rel).or_default().push(format!("toml: {e}"));
                continue;
            }
        };
        let json = match serde_json::to_value(&doc) {
            Ok(v) => v,
            Err(e) => {
                failures
                    .entry(rel)
                    .or_default()
                    .push(format!("toml→json: {e}"));
                continue;
            }
        };

        let errs: Vec<String> = validator
            .iter_errors(&json)
            .map(|e| format!("  {e} at {}", e.instance_path))
            .collect();
        if !errs.is_empty() {
            failures.entry(rel).or_default().extend(errs);
        }
    }

    assert!(
        total > 0,
        "expected at least one rocky.toml under {}; found none",
        root.display()
    );

    assert!(
        failures.is_empty(),
        "generated RockyConfig schema rejects committed POC rocky.toml files:\n{}",
        failures
            .iter()
            .map(|(file, errs)| format!("- {file}\n{}", errs.join("\n")))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
