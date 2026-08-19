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
//! server. They must never instruct a tool that profile excludes; the
//! golden tests below pin that, with one deliberate exception mirroring
//! the MCP instructions banner: the frozen sentence "you cannot and
//! must not propose" names the forbidden act as forbidden.

use std::path::Path;

use anyhow::{Context, Result};

use crate::driver::TaskBriefKind;

const DEFAULT_ELICITATION: &str = include_str!("../briefs/elicitation.md");
const DEFAULT_DRAFTING: &str = include_str!("../briefs/drafting.md");
const DEFAULT_REPAIR: &str = include_str!("../briefs/repair.md");

/// The frozen no-propose sentence every worker-authoring brief carries.
pub const NO_PROPOSE_SENTENCE: &str = "you cannot and must not propose";

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
/// present, the compiled default otherwise.
pub fn load_template(kind: TaskBriefKind, briefs_dir: Option<&Path>) -> Result<String> {
    if let Some(dir) = briefs_dir {
        let candidate = dir.join(format!("{}.md", kind.as_str()));
        if candidate.exists() {
            return std::fs::read_to_string(&candidate)
                .with_context(|| format!("failed to read brief override {}", candidate.display()));
        }
    }
    Ok(default_brief(kind).to_string())
}

/// Render the brief for `kind`: template placeholders substituted from
/// the context. Unknown placeholders are left as-is (an override may
/// carry prose braces); the known set is `{product}`, `{intent}`,
/// `{sources}`, `{model}`, `{outbox_dir}`, `{verify_detail}`.
pub fn render(
    kind: TaskBriefKind,
    briefs_dir: Option<&Path>,
    ctx: &BriefContext,
) -> Result<String> {
    let template = load_template(kind, briefs_dir)?;
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

    /// Tools the worker profile excludes whose names are unambiguous
    /// identifiers: a brief mentioning ANY of them is instructing the
    /// worker toward a tool it cannot have. Mirrors the worker profile's
    /// own excluded-mention golden in rocky-mcp; the authoritative gate
    /// is the integration test that calls them against the real server
    /// and asserts tool-not-found.
    const EXCLUDED_TOOL_IDENTIFIERS: &[&str] = &[
        "review_queue",
        "draft_contract",
        "draft_metadata",
        "pause_schedule",
        "ai_test",
        "ai_contract",
    ];

    fn all_kinds() -> [TaskBriefKind; 3] {
        [
            TaskBriefKind::Elicitation,
            TaskBriefKind::Drafting,
            TaskBriefKind::Repair,
        ]
    }

    #[test]
    fn no_default_brief_names_an_excluded_tool() {
        for kind in all_kinds() {
            let brief = default_brief(kind);
            for tool in EXCLUDED_TOOL_IDENTIFIERS {
                assert!(
                    !brief.contains(tool),
                    "{} brief must not name excluded tool '{tool}'",
                    kind.as_str()
                );
            }
        }
    }

    /// `propose` is both an excluded tool and an English verb. Exactly
    /// two contexts are sanctioned: the frozen negative sentence, and
    /// the spec literal `propose_only` (the trust-dial VALUE the
    /// elicitation worker writes into the candidate — data, not tool
    /// guidance). Every other occurrence is a defect: it would read as
    /// guidance toward a tool the worker cannot have.
    #[test]
    fn propose_appears_only_in_sanctioned_contexts() {
        for kind in all_kinds() {
            let brief = default_brief(kind)
                .replace(NO_PROPOSE_SENTENCE, "")
                .replace("propose_only", "");
            assert!(
                !brief.contains("propose"),
                "{} brief: 'propose' may appear only inside '{NO_PROPOSE_SENTENCE}' or the \
                 spec literal 'propose_only'",
                kind.as_str()
            );
        }
    }

    /// The drafting brief carries the frozen stop condition verbatim.
    #[test]
    fn drafting_brief_carries_the_frozen_stop_sentence() {
        let brief = default_brief(TaskBriefKind::Drafting);
        assert!(brief.contains("Stop when compile/test are green"));
        assert!(brief.contains(NO_PROPOSE_SENTENCE));
        let repair = default_brief(TaskBriefKind::Repair);
        assert!(repair.contains(NO_PROPOSE_SENTENCE));
    }

    #[test]
    fn render_substitutes_the_known_placeholders() {
        let ctx = BriefContext {
            product: "revenue_daily".to_string(),
            intent: "Daily revenue per client".to_string(),
            sources: vec!["poc.raw.stripe_charges".to_string()],
            output_model: "revenue_daily".to_string(),
            outbox_dir: "/tmp/outbox".to_string(),
            verify_detail: String::new(),
        };
        let rendered = render(TaskBriefKind::Elicitation, None, &ctx).expect("renders");
        assert!(rendered.contains("`revenue_daily`"));
        assert!(rendered.contains("- poc.raw.stripe_charges"));
        assert!(rendered.contains("/tmp/outbox/candidate_spec.toml"));
        assert!(!rendered.contains("{product}"));
        assert!(!rendered.contains("{sources}"));
        assert!(!rendered.contains("{outbox_dir}"));
    }

    #[test]
    fn briefs_dir_override_wins_and_absent_files_fall_back() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("drafting.md"), "override for {product}").expect("write");
        let ctx = BriefContext {
            product: "p1".to_string(),
            ..Default::default()
        };
        let rendered = render(TaskBriefKind::Drafting, Some(dir.path()), &ctx).expect("renders");
        assert_eq!(rendered, "override for p1");
        // No elicitation.md in the override dir → the compiled default.
        let fallback = render(TaskBriefKind::Elicitation, Some(dir.path()), &ctx).expect("renders");
        assert!(fallback.contains("candidate product spec"));
    }
}
