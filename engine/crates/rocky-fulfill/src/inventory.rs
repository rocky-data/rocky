//! Route-inventory enforcement: the pinned-API golden and the
//! grep-gates.
//!
//! The structural half of the loop/gate boundary lives in `rocky-cli`
//! (raw plan writers are `pub(crate)`; the compile-fail suite in
//! `rocky-cli-compiletest` pins that they are not nameable from
//! outside). This module is the defense-in-depth half: every
//! `rocky_cli::` / `rocky_core::` path this crate consumes is
//! enumerated in a golden, so an addition is a deliberate,
//! reviewer-visible diff — and a handful of forbidden shapes are
//! grepped for outright.

#![cfg(test)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Every `rocky_cli::` / `rocky_core::` path consumed by this crate's
/// source, as written. Additions are DELIBERATE diffs: call them out in
/// the PR, and justify each one against the fulfill_api façade rule.
const CONSUMED_ENGINE_PATHS: &[&str] = &[
    // The façade — the loop's ONLY rocky-cli entries.
    "rocky_cli::commands::fulfill_api",
    // Config vocabulary (the [fulfill] block + the apply principal).
    "rocky_core::config::FulfillDriverConfig",
    "rocky_core::config::PolicyPrincipal",
    "rocky_core::config::RockyConfig",
    "rocky_core::config::load_rocky_config",
    // The state vocabulary + the CAS store (WP-E1's tables).
    "rocky_core::fulfill::FulfillCas",
    "rocky_core::fulfill::FulfillJournalRow",
    "rocky_core::fulfill::FulfillState",
    "rocky_core::fulfill::FulfillStateRecord",
    "rocky_core::fulfill::FulfillStateRecord::new",
    "rocky_core::fulfill::ProductApprovalRecord",
    "rocky_core::state::StateStore",
    // Spec identity + the confined write target (the runner's candidate
    // write and the snapshot re-verification).
    "rocky_core::product::commit::contained_write_target",
    "rocky_core::product::spec::ParsedSpec",
    "rocky_core::product::spec::parse_spec_bytes",
    "rocky_core::product::spec::spec_digest",
];

/// Read every `.rs` file under `src/`.
fn crate_sources() -> Vec<(PathBuf, String)> {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut out = Vec::new();
    let mut stack = vec![src];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read src dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                let text = std::fs::read_to_string(&path).expect("read source");
                out.push((path, text));
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Extract every `rocky_cli::…` / `rocky_core::…` path mentioned in the
/// sources, expanding one level of `use path::{A, B::C}` groups so the
/// golden names full items, not just group prefixes. This file itself is
/// excluded (the golden would otherwise read its own pin list).
fn consumed_paths() -> BTreeSet<String> {
    let mut found = BTreeSet::new();
    for (path, text) in crate_sources() {
        if path.ends_with("inventory.rs") {
            continue;
        }
        // Comment lines cite paths (machine.rs cites schedule::claim by
        // instruction) without consuming them: the golden pins CODE.
        let code_only: String = text
            .lines()
            .filter(|line| !line.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        collect_from(&code_only, &mut found);
    }
    found
}

fn collect_from(text: &str, found: &mut BTreeSet<String>) {
    let bytes = text.as_bytes();
    let mut i = 0;
    while let Some(offset) = text[i..]
        .find("rocky_cli::")
        .into_iter()
        .chain(text[i..].find("rocky_core::"))
        .min()
    {
        let start = i + offset;
        // Consume the plain `a::b::c` path.
        let mut end = start;
        while end < bytes.len() {
            let c = bytes[end] as char;
            if c.is_ascii_alphanumeric() || c == '_' {
                end += 1;
            } else if c == ':' && bytes.get(end + 1) == Some(&b':') {
                end += 2;
            } else {
                break;
            }
        }
        let mut base = text[start..end].trim_end_matches("::").to_string();
        // A `use base::{A, B as X, C::D}` group: expand one level. A
        // façade-module group still collapses onto the module — the
        // golden pins the DOOR, not each handle on it.
        if bytes.get(end) == Some(&b'{') {
            let close = text[end..].find('}').map(|c| end + c);
            if let Some(close) = close {
                if base == "rocky_cli::commands::fulfill_api"
                    || base.starts_with("rocky_cli::commands::fulfill_api::")
                {
                    found.insert("rocky_cli::commands::fulfill_api".to_string());
                } else {
                    for item in text[end + 1..close].split(',') {
                        let item = item.trim();
                        if item.is_empty() {
                            continue;
                        }
                        let item = item.split_whitespace().next().unwrap_or(item);
                        if item == "self" {
                            found.insert(base.clone());
                        } else {
                            found.insert(format!("{base}::{item}"));
                        }
                    }
                }
                i = close + 1;
                continue;
            }
        }
        // `rocky_cli::commands::fulfill_api::foo` collapses onto the
        // façade module: the golden pins the MODULE as the entry, and
        // itemized façade symbols would double-count the same door.
        if base.starts_with("rocky_cli::commands::fulfill_api::") {
            base = "rocky_cli::commands::fulfill_api".to_string();
        }
        found.insert(base);
        i = end;
    }
}

#[test]
fn the_route_inventory_golden_matches_the_source() {
    let consumed = consumed_paths();
    let pinned: BTreeSet<String> = CONSUMED_ENGINE_PATHS
        .iter()
        .map(ToString::to_string)
        .collect();
    let unpinned: Vec<&String> = consumed.difference(&pinned).collect();
    let stale: Vec<&String> = pinned.difference(&consumed).collect();
    assert!(
        unpinned.is_empty() && stale.is_empty(),
        "route inventory drift.\nconsumed but not pinned (add DELIBERATELY, call out in \
         the PR): {unpinned:?}\npinned but no longer consumed (remove): {stale:?}"
    );
}

/// Grep-gates: shapes this crate must never contain, per the
/// loop/gate boundary. Checked over raw source text (comments
/// included — a comment telling someone to do these things is drift
/// bait too).
#[test]
fn grep_gates_hold() {
    for (path, text) in crate_sources() {
        if path.ends_with("inventory.rs") {
            continue; // this file names the forbidden strings by design
        }
        let rendered = path.display();
        assert!(
            !text.contains(".rocky/plans"),
            "{rendered}: the loop never touches the plan store's directory directly"
        );
        assert!(
            !text.contains(".reviewed.json"),
            "{rendered}: the loop never names (let alone writes) a review marker file"
        );
        assert!(
            !text.contains("write_plan_governed") && !text.contains("write_plan("),
            "{rendered}: plans are written ONLY inside the shared governed propose helper"
        );
    }
}

/// `apply_plan` (the typed apply core) is invoked from exactly one
/// module: `step.rs`. Everything else may only pass its OUTCOME around.
#[test]
fn apply_is_invoked_from_exactly_one_module() {
    let mut callers: Vec<String> = Vec::new();
    for (path, text) in crate_sources() {
        if path.ends_with("inventory.rs") {
            continue;
        }
        if text.contains("apply_plan(") {
            callers.push(
                path.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            );
        }
    }
    assert_eq!(
        callers,
        vec!["step.rs".to_string()],
        "the typed apply core must be invoked from exactly one module"
    );
}
