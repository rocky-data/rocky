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
//!
//! Honest limits: this is a TEXT scanner, not a resolver. It refuses
//! every alias/rename shape it can see (bare crate tokens, `use … as`
//! on rocky paths, group renames), and it scans code only (comments and
//! string literals are stripped before extraction) — but a macro that
//! ASSEMBLES a path, or generated code, is invisible to it. That class
//! is covered by the structural wall, which is the authoritative one:
//! no public rocky-cli symbol can mint or persist a plan outside the
//! governed helper, and the compile-fail suite pins it.

#![cfg(test)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Every `rocky_cli::` / `rocky_core::` path consumed by this crate's
/// source, as written. Additions are DELIBERATE diffs: call them out in
/// the PR, and justify each one against the fulfill_api façade rule.
const CONSUMED_ENGINE_PATHS: &[&str] = &[
    // The façade — the loop's ONLY rocky-cli entries, at SYMBOL
    // granularity: consuming a new façade item is a golden diff.
    "rocky_cli::commands::fulfill_api",
    "rocky_cli::commands::fulfill_api::FulfillOutput",
    "rocky_cli::commands::fulfill_api::PolicyRefusal",
    "rocky_cli::commands::fulfill_api::ProductBinding",
    "rocky_cli::commands::fulfill_api::ProposeOutcome",
    "rocky_cli::commands::fulfill_api::ProposeRequest",
    "rocky_cli::commands::fulfill_api::ReceiptLookup",
    // #1493 — the drafting-window reopen (typed outcome + the call).
    "rocky_cli::commands::fulfill_api::ReopenOutcome",
    "rocky_cli::commands::fulfill_api::TypedApplyOutcome",
    "rocky_cli::commands::fulfill_api::VerifyStatus",
    "rocky_cli::commands::fulfill_api::apply_plan",
    "rocky_cli::commands::fulfill_api::compile_output",
    "rocky_cli::commands::fulfill_api::compute_review_status",
    // Asks the DECLARATIVE runner's own loader how many checks it would
    // execute, so the deferred count the verify bundle reports is the
    // executed set by construction (it includes `[[use_test]]`
    // expansion, which reading the sidecar's raw array misses).
    "rocky_cli::commands::fulfill_api::declarative_test_count",
    "rocky_cli::commands::fulfill_api::lookup_apply_receipt",
    "rocky_cli::commands::fulfill_api::observe_max_time_column",
    "rocky_cli::commands::fulfill_api::print_json",
    "rocky_cli::commands::fulfill_api::product_approve",
    "rocky_cli::commands::fulfill_api::product_compile",
    "rocky_cli::commands::fulfill_api::product_reopen_drafting",
    "rocky_cli::commands::fulfill_api::product_status",
    "rocky_cli::commands::fulfill_api::product_verify",
    "rocky_cli::commands::fulfill_api::propose_governed_run_plan",
    "rocky_cli::commands::fulfill_api::test_output",
    // Config vocabulary (the [fulfill] block + the apply principal).
    "rocky_core::config::FulfillDriverConfig",
    "rocky_core::config::PolicyPrincipal",
    "rocky_core::config::RockyConfig",
    "rocky_core::config::load_rocky_config",
    // The state vocabulary + the CAS store (WP-E1's tables).
    // `DraftingRound` is the PERSISTED drafting-round identity (#1493):
    // a resume dispatches the round the machine decided, not a default.
    "rocky_core::fulfill::DraftingRound",
    "rocky_core::fulfill::FulfillCas",
    "rocky_core::fulfill::FulfillJournalRow",
    "rocky_core::fulfill::FulfillState",
    "rocky_core::fulfill::FulfillStateRecord",
    "rocky_core::fulfill::FulfillStateRecord::new",
    "rocky_core::fulfill::ProductApprovalRecord",
    "rocky_core::state::StateStore",
    // The process start-time probe (#1493). DELIBERATE addition: it used
    // to live in this crate, and moved DOWN to rocky-core because
    // `rocky-cli` needs the same answer to tell whether a product
    // record's owner stamp is its own process — and rocky-cli cannot
    // depend on this crate, which sits above it. This crate re-exports
    // it, so the ownership probe and every caller are unchanged.
    "rocky_core::process::process_liveness",
    // The worker transcript's O_EXCL no-follow create. Added DELIBERATELY
    // (#1500): the driver used `std::fs::File::create`, which is
    // O_CREAT|O_TRUNC — it follows a symlink at the leaf and truncates
    // through a hardlink, and the transcript name is a plain UTC-second
    // stamp in a directory a lower-privilege process can write. This is the
    // STRICT O_EXCL sibling of `write_new_no_follow`, which this crate
    // already consumes: it surfaces `AlreadyExists` instead of unlinking,
    // because a colliding transcript name may belong to a LIVE peer. The
    // alternative was a second copy of a guard that is easy to get subtly
    // wrong.
    "rocky_core::product::commit::create_new_no_follow_strict",
    // Spec identity + the confined write target (the runner's candidate
    // write and the snapshot re-verification).
    "rocky_core::product::commit::contained_write_target",
    // The staged candidate's tmp is DERIVED from that validated target, so no
    // check has seen it. Written through the same O_EXCL no-follow create the
    // commit module uses for its own scratch, rather than a plain
    // `std::fs::write` that follows a planted link (#1500). Added deliberately:
    // one more function from a module this crate already consumes, and the
    // alternative was a second copy of a guard that is easy to get subtly wrong.
    "rocky_core::product::commit::write_new_no_follow",
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

/// Blank out everything that is not code: line comments, block
/// comments, string literals (escape-aware), and raw string literals
/// (`r"…"` through `r###"…"###`). Citations in docs and paths inside
/// error strings are neither consumption nor violations; the golden
/// pins CODE.
fn strip_comments_and_strings(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = vec![b' '; bytes.len()];
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'/' if bytes.get(i + 1) == Some(&b'/') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i += 2;
                while i < bytes.len() && !(bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/')) {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            b'r' if matches!(bytes.get(i + 1), Some(&b'"') | Some(&b'#')) => {
                // Raw string: r"…" or r#"…"# (count the hashes).
                let mut hashes = 0;
                let mut j = i + 1;
                while bytes.get(j) == Some(&b'#') {
                    hashes += 1;
                    j += 1;
                }
                if bytes.get(j) == Some(&b'"') {
                    j += 1;
                    let terminator: Vec<u8> = std::iter::once(b'"')
                        .chain(std::iter::repeat_n(b'#', hashes))
                        .collect();
                    while j < bytes.len() && !bytes[j..].starts_with(&terminator) {
                        j += 1;
                    }
                    i = (j + terminator.len()).min(bytes.len());
                } else {
                    out[i] = bytes[i];
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1; // skip the escaped byte (incl. \")
                    }
                    i += 1;
                }
                i = (i + 1).min(bytes.len());
            }
            other => {
                out[i] = other;
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Extract every `rocky_cli::…` / `rocky_core::…` path the CODE
/// consumes (comments and string literals stripped first), expanding
/// one level of `use path::{A, B::C}` groups so the golden names full
/// items, not just group prefixes — plus every rename VIOLATION the
/// walk saw (`use <rocky path> as x`, group renames). This file itself
/// is excluded (the golden would otherwise read its own pin list).
fn scan_sources() -> (BTreeSet<String>, Vec<String>) {
    let mut found = BTreeSet::new();
    let mut violations = Vec::new();
    for (path, text) in crate_sources() {
        if path.ends_with("inventory.rs") {
            continue;
        }
        let code_only = strip_comments_and_strings(&text);
        collect_from(&code_only, &mut found, &mut violations);
    }
    (found, violations)
}

/// Alias shapes that would let engine paths hide from the golden:
/// a bare `rocky_cli` / `rocky_core` token not followed by `::`
/// (`use rocky_cli as cli;`, a re-export, a bare crate reference) or a
/// ` as ` rename inside a rocky use-group (`{self as x}`). Any hit is a
/// violation outright — the inventory only stays honest if the crates
/// are always named in full.
fn alias_violations(text: &str) -> Vec<String> {
    let mut violations = Vec::new();
    let bytes = text.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    for token in ["rocky_cli", "rocky_core"] {
        let mut from = 0;
        while let Some(offset) = text[from..].find(token) {
            let start = from + offset;
            let end = start + token.len();
            let bounded = (start == 0 || !is_ident(bytes[start - 1]))
                && (end == bytes.len() || !is_ident(bytes[end]));
            if bounded && !text[end..].starts_with("::") {
                violations.push(format!(
                    "bare `{token}` (alias/rename/re-export?) at byte {start}: \
                     `{}`",
                    text[start..].lines().next().unwrap_or("")
                ));
            }
            from = end;
        }
    }
    violations
}

fn collect_from(text: &str, found: &mut BTreeSet<String>, violations: &mut Vec<String>) {
    let bytes = text.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut i = 0;
    while let Some(offset) = text[i..]
        .find("rocky_cli::")
        .into_iter()
        .chain(text[i..].find("rocky_core::"))
        .chain(text[i..].find("fulfill_api::"))
        .min()
    {
        let start = i + offset;
        // A qualified `…::fulfill_api::x` is handled when the full path
        // is consumed below; only a MODULE-LOCAL `fulfill_api::x` use
        // (no crate prefix) starts here.
        if text[..start].ends_with("::") {
            i = start + 1;
            continue;
        }
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
        let base = normalize(text[start..end].trim_end_matches("::"));
        // A qualified rename (`use rocky_cli::commands::fulfill_api as
        // gate;`) would let every later `gate::X` consumption hide from
        // the golden — refused outright, exactly like the bare-token
        // alias.
        {
            let mut after = end;
            while after < bytes.len() && (bytes[after] as char).is_whitespace() {
                after += 1;
            }
            let renames = text[after..].starts_with("as")
                && bytes.get(after + 2).is_none_or(|b| !is_ident(*b));
            if renames {
                violations.push(format!(
                    "qualified rename of `{}` (`… as …`) hides consumption from the \
                     golden — name engine paths in full",
                    text[start..end].trim_end_matches("::")
                ));
            }
        }
        // A `use base::{A, B as X, C::D}` group: expand one level, at
        // SYMBOL granularity — façade items included, so a NEW façade
        // consumption is a golden diff. A rename inside the group is a
        // violation for the same reason as the qualified rename.
        if bytes.get(end) == Some(&b'{') {
            let close = text[end..].find('}').map(|c| end + c);
            if let Some(close) = close {
                for item in text[end + 1..close].split(',') {
                    let item = item.trim();
                    if item.is_empty() {
                        continue;
                    }
                    if item.contains(" as ") {
                        violations.push(format!(
                            "use-group rename `{item}` hides a path from the golden — \
                             name engine items in full"
                        ));
                    }
                    let item = item.split_whitespace().next().unwrap_or(item);
                    if item == "self" {
                        found.insert(base.clone());
                    } else {
                        found.insert(normalize(&format!("{base}::{item}")));
                    }
                }
                i = close + 1;
                continue;
            }
        }
        found.insert(base);
        i = end;
    }
}

/// Canonical golden spelling: module-local `fulfill_api::X` and the
/// fully-qualified spelling pin identically, and a path deeper than one
/// item past the façade (`fulfill_api::ProposeOutcome::Written`) pins
/// its first item — the SYMBOL is the unit, variants ride their type.
fn normalize(path: &str) -> String {
    let path = if let Some(rest) = path.strip_prefix("fulfill_api::") {
        format!("rocky_cli::commands::fulfill_api::{rest}")
    } else if path == "fulfill_api" {
        "rocky_cli::commands::fulfill_api".to_string()
    } else {
        path.to_string()
    };
    if let Some(rest) = path.strip_prefix("rocky_cli::commands::fulfill_api::") {
        let first = rest.split("::").next().unwrap_or(rest);
        return format!("rocky_cli::commands::fulfill_api::{first}");
    }
    path
}

/// Whether `text` calls `ident` as a function, tolerating any
/// whitespace between the identifier and its `(` — `apply_plan (`,
/// `apply_plan\n(`, and the plain spelling all match; identifier
/// boundaries keep `x_apply_plan(` out.
fn calls_identifier(text: &str, ident: &str) -> bool {
    let bytes = text.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut from = 0;
    while let Some(offset) = text[from..].find(ident) {
        let start = from + offset;
        let end = start + ident.len();
        let bounded = (start == 0 || !is_ident(bytes[start - 1]))
            && (end == bytes.len() || !is_ident(bytes[end]));
        if bounded {
            let mut rest = end;
            while rest < bytes.len() && (bytes[rest] as char).is_whitespace() {
                rest += 1;
            }
            if bytes.get(rest) == Some(&b'(') {
                return true;
            }
        }
        from = end;
    }
    false
}

/// Whether `text` names any identifier that STARTS with `prefix` at an
/// identifier start boundary (`write_plan`, `write_plan_governed`,
/// `write_plan_with_principal`, …) — call or path mention alike.
fn names_identifier_prefix(text: &str, prefix: &str) -> bool {
    let bytes = text.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut from = 0;
    while let Some(offset) = text[from..].find(prefix) {
        let start = from + offset;
        if start == 0 || !is_ident(bytes[start - 1]) {
            return true;
        }
        from = start + prefix.len();
    }
    false
}

/// Alias shapes hide paths from the golden — none may exist anywhere
/// in the crate: bare crate tokens, qualified renames
/// (`use rocky_cli::… as gate;`), and use-group renames, all checked on
/// the code the extraction scan walks (comments and strings stripped —
/// prose cannot compile, and paths inside error strings are neither
/// consumption nor evasion).
#[test]
fn no_engine_crate_is_aliased_or_bare() {
    for (path, text) in crate_sources() {
        if path.ends_with("inventory.rs") {
            continue;
        }
        let violations = alias_violations(&strip_comments_and_strings(&text));
        assert!(violations.is_empty(), "{}: {violations:?}", path.display());
    }
    let (_, rename_violations) = scan_sources();
    assert!(
        rename_violations.is_empty(),
        "rename violations: {rename_violations:?}"
    );
}

#[test]
fn the_route_inventory_golden_matches_the_source() {
    let (consumed, _) = scan_sources();
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
            !names_identifier_prefix(&text, "write_plan"),
            "{rendered}: plans are written ONLY inside the shared governed propose helper \
             (no write_plan* identifier may even be named here)"
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
        if calls_identifier(&text, "apply_plan") {
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
