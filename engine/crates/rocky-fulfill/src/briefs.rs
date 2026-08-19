//! Prompts as data: the worker task briefs.
//!
//! Compiled defaults live under `rocky-fulfill/briefs/` and are
//! embedded via `include_str!` (the MCP INSTRUCTIONS precedent), so
//! brief iteration needs no engine release when a project overrides
//! them: a file named `elicitation.md` / `drafting.md` / `repair.md`
//! inside `[fulfill] briefs_dir` replaces the compiled default of the
//! same name; absent files fall back.
//!
//! Briefs address the WORKER, which runs on the worker-profile MCP
//! server. They must never instruct a tool that profile excludes.
//! [`validate_brief`] enforces that on EVERY loaded template — compiled
//! default and `briefs_dir` override alike — with one deliberate,
//! byte-anchored exception mirroring the MCP instructions banner: the
//! frozen sentence "you cannot and must not propose" names the
//! forbidden act as forbidden. Overrides resolve through the same
//! containment primitive the staged commit uses, so `briefs_dir` can
//! never name an absolute path, traverse out of the project, or ride a
//! symlinked ancestor.

use std::path::Path;

use anyhow::{Context, Result, bail};

use crate::driver::TaskBriefKind;

const DEFAULT_ELICITATION: &str = include_str!("../briefs/elicitation.md");
const DEFAULT_DRAFTING: &str = include_str!("../briefs/drafting.md");
const DEFAULT_REPAIR: &str = include_str!("../briefs/repair.md");

/// The frozen no-propose sentence every worker-authoring brief carries.
pub const NO_PROPOSE_SENTENCE: &str = "you cannot and must not propose";

/// Tool names the worker profile does not serve. No brief may name any
/// of them as a word (case-insensitive, identifier-boundary matched) —
/// a brief steering the worker toward a tool it cannot have is drift
/// bait, whether it came compiled-in or from an override. The one
/// sanctioned exception: the exact [`NO_PROPOSE_SENTENCE`] bytes.
/// Mirrors the worker profile's own excluded-mention golden in
/// rocky-mcp; the authoritative runtime gate is the server itself
/// (tool-not-found), exercised by the integration battery.
pub const EXCLUDED_WORKER_TOOLS: &[&str] = &[
    "propose",
    "apply",
    "review_queue",
    "draft_contract",
    "draft_metadata",
    "pause_schedule",
    "ai_test",
    "ai_contract",
];

/// Refuse a template that names an excluded worker tool.
///
/// The check strips every byte-exact occurrence of the frozen
/// [`NO_PROPOSE_SENTENCE`] first (the anchored carve-out), lowercases
/// the rest, and matches each excluded name at identifier boundaries —
/// so `You must Propose` is refused (case dodges do not pass), while
/// `applying` and the spec literal `propose_only` are not matches (the
/// boundary is any `[a-z0-9_]` neighbour).
pub fn validate_brief(kind: TaskBriefKind, template: &str) -> Result<()> {
    let stripped = template.replace(NO_PROPOSE_SENTENCE, " ");
    let lowered = stripped.to_lowercase();
    for tool in EXCLUDED_WORKER_TOOLS {
        if contains_identifier(&lowered, tool) {
            bail!(
                "the {} brief names the excluded worker tool '{tool}' outside the frozen \
                 sentence '{NO_PROPOSE_SENTENCE}' — a worker brief must not steer toward a \
                 tool the worker profile does not serve; reword the brief",
                kind.as_str()
            );
        }
    }
    Ok(())
}

/// Whether `needle` (lowercase) occurs in `haystack` (lowercase) as an
/// identifier: neither neighbour is `[a-z0-9_]`.
fn contains_identifier(haystack: &str, needle: &str) -> bool {
    let bytes = haystack.as_bytes();
    let is_ident = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_';
    let mut from = 0;
    while let Some(offset) = haystack[from..].find(needle) {
        let start = from + offset;
        let end = start + needle.len();
        let before_ok = start == 0 || !is_ident(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_ident(bytes[end]);
        if before_ok && after_ok {
            return true;
        }
        from = start + 1;
    }
    false
}

/// Everything a brief render can substitute.
#[derive(Debug, Clone, Default)]
pub struct BriefContext {
    /// The product name.
    pub product: String,
    /// The spec's intent, verbatim.
    pub intent: String,
    /// The exact source refs, one per line.
    pub sources: Vec<String>,
    /// The output model name.
    pub output_model: String,
    /// The task outbox directory, rendered absolute.
    pub outbox_dir: String,
    /// The red verification detail (repair briefs).
    pub verify_detail: String,
}

/// The compiled default for `kind`.
pub fn default_brief(kind: TaskBriefKind) -> &'static str {
    match kind {
        TaskBriefKind::Elicitation => DEFAULT_ELICITATION,
        TaskBriefKind::Drafting => DEFAULT_DRAFTING,
        TaskBriefKind::Repair => DEFAULT_REPAIR,
    }
}

/// Load the brief template for `kind`: the `briefs_dir` override when
/// present, the compiled default otherwise. Every loaded template —
/// override AND default — passes [`validate_brief`] before it is
/// returned; a template naming an excluded tool is a typed refusal,
/// never a dispatch.
///
/// `briefs_dir` is resolved through the staged commit's containment
/// primitive against `project_root`: an absolute path, a traversing
/// path, or a symlinked-ancestor escape is refused outright.
pub fn load_template(
    kind: TaskBriefKind,
    project_root: &Path,
    briefs_dir: Option<&Path>,
) -> Result<String> {
    let template = match briefs_dir {
        Some(dir) => {
            let rel = format!("{}/{}.md", dir.display(), kind.as_str());
            let candidate = rocky_core::product::commit::contained_write_target(project_root, &rel)
                .map_err(|err| {
                    anyhow::anyhow!(
                        "[fulfill] briefs_dir is not a contained project path: {err} — \
                             briefs_dir must be a plain relative directory inside the project"
                    )
                })?;
            if candidate.exists() {
                std::fs::read_to_string(&candidate).with_context(|| {
                    format!("failed to read brief override {}", candidate.display())
                })?
            } else {
                default_brief(kind).to_string()
            }
        }
        None => default_brief(kind).to_string(),
    };
    validate_brief(kind, &template)?;
    Ok(template)
}

/// Render the brief for `kind`: template placeholders substituted from
/// the context. Unknown placeholders are left as-is (an override may
/// carry prose braces); the known set is `{product}`, `{intent}`,
/// `{sources}`, `{model}`, `{outbox_dir}`, `{verify_detail}`.
///
/// Validation runs on the TEMPLATE, before substitution: the context's
/// `intent`/`verify_detail` are quoted data (the spec's own words, the
/// verifier's own findings), not instructions this crate authors.
pub fn render(
    kind: TaskBriefKind,
    project_root: &Path,
    briefs_dir: Option<&Path>,
    ctx: &BriefContext,
) -> Result<String> {
    let template = load_template(kind, project_root, briefs_dir)?;
    let sources = if ctx.sources.is_empty() {
        "(none listed)".to_string()
    } else {
        ctx.sources
            .iter()
            .map(|s| format!("- {s}"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    Ok(template
        .replace("{product}", &ctx.product)
        .replace("{intent}", &ctx.intent)
        .replace("{sources}", &sources)
        .replace("{model}", &ctx.output_model)
        .replace("{outbox_dir}", &ctx.outbox_dir)
        .replace("{verify_detail}", &ctx.verify_detail))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_kinds() -> [TaskBriefKind; 3] {
        [
            TaskBriefKind::Elicitation,
            TaskBriefKind::Drafting,
            TaskBriefKind::Repair,
        ]
    }

    /// Every compiled default passes the SAME validation the loader
    /// applies at runtime — the goldens and the gate share one rule.
    #[test]
    fn every_default_brief_validates() {
        for kind in all_kinds() {
            validate_brief(kind, default_brief(kind)).expect("default brief must validate");
        }
    }

    /// The drafting and repair briefs carry the frozen stop condition
    /// verbatim.
    #[test]
    fn drafting_and_repair_carry_the_frozen_stop_sentence() {
        let brief = default_brief(TaskBriefKind::Drafting);
        assert!(brief.contains("Stop when compile/test are green"));
        assert!(brief.contains(NO_PROPOSE_SENTENCE));
        assert!(default_brief(TaskBriefKind::Repair).contains(NO_PROPOSE_SENTENCE));
    }

    /// The carve-out is byte-anchored and matching is case-insensitive:
    /// a case-dodged instruction is refused; the exact frozen sentence
    /// (and the spec literal `propose_only`) are not.
    #[test]
    fn validation_is_case_insensitive_and_the_carve_out_is_exact() {
        // Case dodge → refused.
        let err = validate_brief(TaskBriefKind::Drafting, "You must Propose the plan.")
            .expect_err("case dodge");
        assert!(format!("{err:#}").contains("'propose'"), "{err:#}");
        // The exact frozen sentence alone → fine.
        validate_brief(
            TaskBriefKind::Drafting,
            &format!("Work, then stop; {NO_PROPOSE_SENTENCE}."),
        )
        .expect("the anchored carve-out");
        // A near-miss of the sentence is NOT carved out.
        let err = validate_brief(
            TaskBriefKind::Drafting,
            "you cannot and must not Propose", // case-broken sentence
        )
        .expect_err("a byte-inexact sentence earns no carve-out");
        assert!(format!("{err:#}").contains("'propose'"), "{err:#}");
        // The spec literal and morphological neighbours are not matches.
        validate_brief(
            TaskBriefKind::Elicitation,
            "set agent = \"propose_only\" and keep applying yourself",
        )
        .expect("identifier boundaries hold");
    }

    /// Every excluded tool name is refused as a word, `apply` included.
    #[test]
    fn every_excluded_tool_name_is_refused() {
        for tool in EXCLUDED_WORKER_TOOLS {
            let template = format!("please call {tool} now");
            let err = validate_brief(TaskBriefKind::Drafting, &template)
                .expect_err("excluded tool must refuse");
            assert!(format!("{err:#}").contains(&format!("'{tool}'")), "{err:#}");
        }
    }

    #[test]
    fn render_substitutes_the_known_placeholders() {
        let root = tempfile::tempdir().expect("tempdir");
        let ctx = BriefContext {
            product: "revenue_daily".to_string(),
            intent: "Daily revenue per client".to_string(),
            sources: vec!["poc.raw.stripe_charges".to_string()],
            output_model: "revenue_daily".to_string(),
            outbox_dir: "/tmp/outbox".to_string(),
            verify_detail: String::new(),
        };
        let rendered =
            render(TaskBriefKind::Elicitation, root.path(), None, &ctx).expect("renders");
        assert!(rendered.contains("`revenue_daily`"));
        assert!(rendered.contains("- poc.raw.stripe_charges"));
        assert!(rendered.contains("/tmp/outbox/candidate_spec.toml"));
        assert!(!rendered.contains("{product}"));
        assert!(!rendered.contains("{sources}"));
        assert!(!rendered.contains("{outbox_dir}"));
    }

    #[test]
    fn briefs_dir_override_wins_and_absent_files_fall_back() {
        let root = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(root.path().join("briefs")).expect("mkdir");
        std::fs::write(
            root.path().join("briefs/drafting.md"),
            "override for {product}",
        )
        .expect("write");
        let ctx = BriefContext {
            product: "p1".to_string(),
            ..Default::default()
        };
        let rendered = render(
            TaskBriefKind::Drafting,
            root.path(),
            Some(Path::new("briefs")),
            &ctx,
        )
        .expect("renders");
        assert_eq!(rendered, "override for p1");
        // No elicitation.md in the override dir → the compiled default.
        let fallback = render(
            TaskBriefKind::Elicitation,
            root.path(),
            Some(Path::new("briefs")),
            &ctx,
        )
        .expect("renders");
        assert!(fallback.contains("candidate product spec"));
    }

    /// F2: an override brief passes the SAME exclusion validation as the
    /// defaults — a steering override is a typed refusal, not a dispatch.
    #[test]
    fn a_steering_override_is_refused_at_load() {
        let root = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(root.path().join("briefs")).expect("mkdir");
        std::fs::write(
            root.path().join("briefs/drafting.md"),
            "Draft, then You must Propose the plan yourself.",
        )
        .expect("write");
        let err = load_template(
            TaskBriefKind::Drafting,
            root.path(),
            Some(Path::new("briefs")),
        )
        .expect_err("steering override");
        assert!(format!("{err:#}").contains("'propose'"), "{err:#}");
    }

    /// F2: briefs_dir is contained — absolute paths and traversals are
    /// refused through the staged commit's own containment primitive.
    #[test]
    fn briefs_dir_refuses_absolute_and_traversing_paths() {
        let root = tempfile::tempdir().expect("tempdir");
        let outside = tempfile::tempdir().expect("outside");
        std::fs::write(outside.path().join("drafting.md"), "hostile {product}").expect("write");

        // Absolute → refused, even though the file exists.
        let err = load_template(TaskBriefKind::Drafting, root.path(), Some(outside.path()))
            .expect_err("absolute briefs_dir");
        assert!(
            format!("{err:#}").contains("absolute"),
            "names the refusal: {err:#}"
        );

        // Traversal → refused.
        let err = load_template(
            TaskBriefKind::Drafting,
            root.path(),
            Some(Path::new("../outside")),
        )
        .expect_err("traversing briefs_dir");
        assert!(
            format!("{err:#}").contains("non-canonical or traversing"),
            "names the refusal: {err:#}"
        );
    }
}
