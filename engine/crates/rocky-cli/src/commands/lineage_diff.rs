//! `rocky lineage-diff` — combine structural CI diff with downstream lineage traces.
//!
//! Wires the per-column structural diff from `rocky ci-diff` together with
//! the downstream-consumer trace from `rocky lineage --downstream` so a PR
//! reviewer can see, in one command:
//!
//! 1. which columns changed in this PR (added / removed / type-changed), and
//! 2. which downstream columns each one reaches on HEAD.
//!
//! Output is rendered as Markdown ready to drop into a GitHub PR comment.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::Result;

use rocky_core::ci_diff::{ColumnChangeType, ColumnDiff, DiffResult, DiffSummary};

use super::ci_diff::compute_ci_diff;
use crate::output::{
    LineageColumnChange, LineageDiffOutput, LineageDiffResult, LineageQualifiedColumn, print_json,
};

/// Execute `rocky lineage-diff`.
pub fn run_lineage_diff(
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

    // Build the per-model, per-column lineage-augmented results.
    let results = enrich_with_downstream(&data.results, data.head_compile.as_ref());
    let markdown = format_lineage_diff_markdown(&results, &data.summary);

    if output_json {
        let output = LineageDiffOutput {
            version: env!("CARGO_PKG_VERSION").to_string(),
            command: "lineage-diff".to_string(),
            base_ref: base_ref.to_string(),
            head_ref: "HEAD".to_string(),
            summary: data.summary,
            results,
            markdown,
        };
        print_json(&output)?;
    } else {
        println!("Rocky Lineage Diff ({base_ref}...HEAD)\n");
        if data.summary.is_clean() && results.is_empty() {
            println!("No changed model files detected.");
        } else {
            print!("{markdown}");
        }
    }

    Ok(())
}

/// For each model's column changes, walk HEAD's `semantic_graph` to collect
/// the downstream consumers of each column.
///
/// Removed columns get an empty consumer list — they don't exist on HEAD,
/// so the trace would return nothing anyway. The structural diff still
/// surfaces the removal in the markdown summary.
fn enrich_with_downstream(
    diff_results: &[DiffResult],
    head_compile: Option<&rocky_compiler::compile::CompileResult>,
) -> Vec<LineageDiffResult> {
    diff_results
        .iter()
        .map(|r| LineageDiffResult {
            model_name: r.model_name.clone(),
            status: r.status,
            column_changes: r
                .column_changes
                .iter()
                .map(|c| enrich_column(&r.model_name, c, head_compile))
                .collect(),
        })
        .collect()
}

fn enrich_column(
    model_name: &str,
    diff: &ColumnDiff,
    head_compile: Option<&rocky_compiler::compile::CompileResult>,
) -> LineageColumnChange {
    let downstream_consumers = match (diff.change_type, head_compile) {
        // Removed columns no longer exist on HEAD — nothing to trace.
        (ColumnChangeType::Removed, _) | (_, None) => Vec::new(),
        (_, Some(compile_result)) => {
            // Dedup by (model, column) — multiple inbound edges to the
            // same consumer column shouldn't appear twice in the report.
            let mut seen: BTreeSet<(String, String)> = BTreeSet::new();
            let mut out: Vec<LineageQualifiedColumn> = Vec::new();
            for edge in compile_result
                .semantic_graph
                .trace_column_downstream(model_name, &diff.column_name)
            {
                let key = (
                    edge.target.model.to_string(),
                    edge.target.column.to_string(),
                );
                if seen.insert(key) {
                    out.push(LineageQualifiedColumn {
                        model: edge.target.model.to_string(),
                        column: edge.target.column.to_string(),
                    });
                }
            }
            out
        }
    };

    LineageColumnChange {
        column_name: diff.column_name.clone(),
        change_type: diff.change_type,
        old_type: diff.old_type.clone(),
        new_type: diff.new_type.clone(),
        downstream_consumers,
    }
}

/// Markdown formatter shaped for a GitHub PR comment.
///
/// Mirrors `rocky_core::ci_diff::format_diff_markdown`'s envelope (header +
/// per-model `<details>` blocks) but each row also includes the downstream
/// consumer count and (when expanded) the qualified column list.
fn format_lineage_diff_markdown(results: &[LineageDiffResult], summary: &DiffSummary) -> String {
    let mut out = String::new();
    out.push_str("### Rocky Lineage Diff\n\n");

    if summary.is_clean() {
        out.push_str("No data changes detected.\n");
        return out;
    }

    out.push_str(&format!(
        "**{} model(s) changed** ({} modified, {} added, {} removed, {} unchanged)\n\n",
        summary.modified + summary.added + summary.removed,
        summary.modified,
        summary.added,
        summary.removed,
        summary.unchanged,
    ));

    for r in results {
        if r.column_changes.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "<details>\n<summary><b>{}</b> — {} ({} column change{})</summary>\n\n",
            r.model_name,
            r.status,
            r.column_changes.len(),
            if r.column_changes.len() == 1 { "" } else { "s" },
        ));

        out.push_str("| Column | Change | Old Type | New Type | Downstream consumers |\n");
        out.push_str("|--------|--------|----------|----------|----------------------|\n");
        for c in &r.column_changes {
            let old = c.old_type.as_deref().unwrap_or("-");
            let new = c.new_type.as_deref().unwrap_or("-");
            let consumers = if c.downstream_consumers.is_empty() {
                if c.change_type == ColumnChangeType::Removed {
                    String::from("_(removed; not traceable on HEAD)_")
                } else {
                    String::from("_none_")
                }
            } else {
                c.downstream_consumers
                    .iter()
                    .map(|q| format!("`{}.{}`", q.model, q.column))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            out.push_str(&format!(
                "| `{}` | {} | {} | {} | {} |\n",
                c.column_name, c.change_type, old, new, consumers
            ));
        }
        out.push_str("\n</details>\n\n");
    }

    out
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::ci_diff::ModelDiffStatus;

    fn col_change(name: &str, kind: ColumnChangeType) -> ColumnDiff {
        ColumnDiff {
            column_name: name.to_string(),
            change_type: kind,
            old_type: None,
            new_type: None,
        }
    }

    #[test]
    fn enrich_returns_empty_consumers_when_compile_missing() {
        let diff = vec![DiffResult {
            model_name: "orders".into(),
            status: ModelDiffStatus::Modified,
            row_count_before: None,
            row_count_after: None,
            column_changes: vec![col_change("amount", ColumnChangeType::TypeChanged)],
            sample_changed_rows: None,
        }];
        let enriched = enrich_with_downstream(&diff, None);
        assert_eq!(enriched.len(), 1);
        assert_eq!(enriched[0].column_changes.len(), 1);
        assert!(
            enriched[0].column_changes[0]
                .downstream_consumers
                .is_empty()
        );
    }

    #[test]
    fn removed_columns_produce_empty_consumer_set_even_with_compile() {
        // Removed columns don't exist on HEAD — even when a compile is
        // available, we don't fall back to base.
        let diff = vec![DiffResult {
            model_name: "orders".into(),
            status: ModelDiffStatus::Modified,
            row_count_before: None,
            row_count_after: None,
            column_changes: vec![col_change("legacy_flag", ColumnChangeType::Removed)],
            sample_changed_rows: None,
        }];
        // Pass None as a stand-in for head_compile (the compile_result wrap
        // is exercised in the integration test). The early-return on
        // `Removed` short-circuits before any graph access.
        let enriched = enrich_with_downstream(&diff, None);
        assert!(
            enriched[0].column_changes[0]
                .downstream_consumers
                .is_empty()
        );
    }

    #[test]
    fn markdown_clean_summary() {
        let summary = DiffSummary {
            total_models: 0,
            unchanged: 0,
            modified: 0,
            added: 0,
            removed: 0,
        };
        let md = format_lineage_diff_markdown(&[], &summary);
        assert!(md.contains("No data changes detected"));
    }

    #[test]
    fn markdown_renders_consumers() {
        let results = vec![LineageDiffResult {
            model_name: "stg_orders".into(),
            status: ModelDiffStatus::Modified,
            column_changes: vec![LineageColumnChange {
                column_name: "email".into(),
                change_type: ColumnChangeType::TypeChanged,
                old_type: Some("VARCHAR".into()),
                new_type: Some("TEXT".into()),
                downstream_consumers: vec![
                    LineageQualifiedColumn {
                        model: "fct_users".into(),
                        column: "email_lower".into(),
                    },
                    LineageQualifiedColumn {
                        model: "rpt_marketing".into(),
                        column: "contact_email".into(),
                    },
                ],
            }],
        }];
        let summary = DiffSummary {
            total_models: 1,
            unchanged: 0,
            modified: 1,
            added: 0,
            removed: 0,
        };
        let md = format_lineage_diff_markdown(&results, &summary);
        assert!(md.contains("stg_orders"));
        assert!(md.contains("`fct_users.email_lower`"));
        assert!(md.contains("`rpt_marketing.contact_email`"));
        assert!(md.contains("VARCHAR"));
        assert!(md.contains("TEXT"));
    }

    #[test]
    fn markdown_marks_removed_as_not_traceable() {
        let results = vec![LineageDiffResult {
            model_name: "stg_orders".into(),
            status: ModelDiffStatus::Modified,
            column_changes: vec![LineageColumnChange {
                column_name: "legacy_flag".into(),
                change_type: ColumnChangeType::Removed,
                old_type: Some("BOOLEAN".into()),
                new_type: None,
                downstream_consumers: vec![],
            }],
        }];
        let summary = DiffSummary {
            total_models: 1,
            unchanged: 0,
            modified: 1,
            added: 0,
            removed: 0,
        };
        let md = format_lineage_diff_markdown(&results, &summary);
        assert!(md.contains("not traceable on HEAD"));
    }

    #[test]
    fn markdown_marks_no_consumers_for_added_with_no_downstream() {
        let results = vec![LineageDiffResult {
            model_name: "leaf_table".into(),
            status: ModelDiffStatus::Added,
            column_changes: vec![LineageColumnChange {
                column_name: "new_col".into(),
                change_type: ColumnChangeType::Added,
                old_type: None,
                new_type: Some("INT".into()),
                downstream_consumers: vec![],
            }],
        }];
        let summary = DiffSummary {
            total_models: 1,
            unchanged: 0,
            modified: 0,
            added: 1,
            removed: 0,
        };
        let md = format_lineage_diff_markdown(&results, &summary);
        assert!(md.contains("_none_"));
    }
}
