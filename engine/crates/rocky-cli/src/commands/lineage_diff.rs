//! `rocky lineage-diff` — combine structural CI diff with downstream lineage traces.
//!
//! Wires the per-column structural diff from `rocky ci-diff` together with
//! the downstream-consumer trace from `rocky lineage --downstream` so a PR
//! reviewer can see, in one command:
//!
//! 1. which columns changed in this PR (added / removed / type-changed),
//! 2. which downstream columns each one reaches on HEAD, and
//! 3. for a removed (or renamed-away) column, what happened to every model
//!    that read it: `repaired`, `deleted`, `newly_broken` or `unknown`.
//!
//! Output is rendered as Markdown ready to drop into a GitHub PR comment.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::Result;

use rocky_compiler::compile::CompileResult;
use rocky_compiler::semantic::SemanticGraph;
use rocky_core::ci_diff::{ColumnChangeType, ColumnDiff, DiffResult, DiffSummary};

use super::ci_diff::{CiDiffMode, compute_ci_diff};
use crate::output::{
    ConsumerImpactStatus, LineageColumnChange, LineageConsumerImpact, LineageDiffOutput,
    LineageDiffResult, LineageQualifiedColumn, print_json,
};

/// Execute `rocky lineage-diff`.
pub fn run_lineage_diff(
    config_path: &Path,
    state_path: &Path,
    base_ref: &str,
    models_dir: &Path,
    output_json: bool,
    cache_ttl_override: Option<u64>,
    mode: CiDiffMode,
) -> Result<()> {
    let data = compute_ci_diff(
        config_path,
        state_path,
        base_ref,
        models_dir,
        cache_ttl_override,
        mode,
    )?;

    let mode = data.mode;
    // Build the per-model, per-column lineage-augmented results.
    let results = enrich_with_downstream(
        &data.results,
        data.head_compile.as_ref(),
        data.base_compile.as_ref(),
    );
    let markdown = format_lineage_diff_markdown(&results, &data.summary);

    if output_json {
        let output = LineageDiffOutput {
            version: env!("CARGO_PKG_VERSION").to_string(),
            command: "lineage-diff".to_string(),
            base_ref: base_ref.to_string(),
            head_ref: mode.head_label().to_string(),
            summary: data.summary,
            results,
            markdown,
            mode,
            base_commit: data.base_commit,
        };
        print_json(&output)?;
    } else {
        println!("Rocky Lineage Diff ({base_ref}...{})\n", mode.head_label());
        if data.summary.is_clean() && results.is_empty() {
            println!("No changed model files detected.");
        } else {
            print!("{markdown}");
        }
    }

    Ok(())
}

/// For each model's column changes, walk HEAD's `semantic_graph` to collect
/// the downstream consumers of each column, and classify the consumers of
/// every removed column by comparing the base and HEAD graphs.
fn enrich_with_downstream(
    diff_results: &[DiffResult],
    head_compile: Option<&CompileResult>,
    base_compile: Option<&CompileResult>,
) -> Vec<LineageDiffResult> {
    diff_results
        .iter()
        .map(|r| LineageDiffResult {
            model_name: r.model_name.clone(),
            status: r.status,
            column_changes: r
                .column_changes
                .iter()
                .map(|c| enrich_column(&r.model_name, c, head_compile, base_compile))
                .collect(),
        })
        .collect()
}

fn enrich_column(
    model_name: &str,
    diff: &ColumnDiff,
    head_compile: Option<&CompileResult>,
    base_compile: Option<&CompileResult>,
) -> LineageColumnChange {
    let downstream_consumers = match (diff.change_type, head_compile) {
        // Removed columns no longer exist on HEAD — nothing to trace there.
        // Their consumers are classified in `consumer_impact` instead.
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

    let consumer_impact = if diff.change_type == ColumnChangeType::Removed {
        classify_removed_column_consumers(model_name, &diff.column_name, base_compile, head_compile)
    } else {
        Vec::new()
    };

    LineageColumnChange {
        column_name: diff.column_name.clone(),
        change_type: diff.change_type,
        old_type: diff.old_type.clone(),
        new_type: diff.new_type.clone(),
        downstream_consumers,
        consumer_impact,
    }
}

// ---------------------------------------------------------------------------
// Consumer classification for removed columns
// ---------------------------------------------------------------------------

/// The direct reads of one column by one consumer model, on one side.
#[derive(Default)]
struct Reads {
    /// Consumer output columns (value edges and window keys).
    columns: BTreeSet<String>,
    /// `value` or a row-selection kind.
    via: BTreeSet<String>,
}

impl Reads {
    fn is_empty(&self) -> bool {
        self.via.is_empty()
    }
}

/// Every direct consumer of `(model, column)` in `graph`, keyed by consumer
/// model: value edges plus row-selection edges.
fn direct_reads(graph: &SemanticGraph, model: &str, column: &str) -> BTreeMap<String, Reads> {
    let mut out: BTreeMap<String, Reads> = BTreeMap::new();
    for edge in graph.column_consumers(model, column) {
        let reads = out.entry(edge.target.model.to_string()).or_default();
        reads.columns.insert(edge.target.column.to_string());
        reads.via.insert("value".to_string());
    }
    for edge in graph.row_selection_consumers(model, column) {
        let reads = out.entry(edge.target_model.to_string()).or_default();
        if let Some(c) = &edge.target_column {
            reads.columns.insert(c.to_string());
        }
        reads.via.insert(edge.kind.to_string());
    }
    out.remove(model);
    out
}

/// Whether a HEAD consumer that has no lineage edge from the removed column
/// might still read it in a place lineage cannot attribute.
///
/// Returns `Some(reason)` when it might (classify `unknown`), `None` when it
/// provably does not (classify `repaired`).
fn unattributed_mention(
    sql: &str,
    upstream: &str,
    column: &str,
    head_graph: &SemanticGraph,
) -> Option<String> {
    let lineage = match rocky_sql::lineage::extract_lineage(sql) {
        Ok(l) => l,
        Err(e) => return Some(format!("lineage could not parse the HEAD SQL ({e})")),
    };
    let names_upstream = |name: &str| name.eq_ignore_ascii_case(upstream);
    let top_level_reads = lineage.source_tables.iter().any(|t| {
        t.binding == rocky_sql::lineage::TableBinding::Physical && names_upstream(&t.name)
    });
    let reads_upstream = top_level_reads
        || lineage.nested_sources.iter().any(|n| names_upstream(n))
        || lineage
            .source_tables
            .iter()
            .any(|t| t.derived_sources.iter().any(|n| names_upstream(n)));
    if !reads_upstream {
        // The consumer no longer reads the upstream relation at all.
        return None;
    }

    // A star over the upstream exposes the removed column only if the
    // upstream still has it; that is decidable only when HEAD knows the
    // upstream's complete schema.
    if lineage.has_star && top_level_reads {
        let schema_complete = head_graph
            .model_schema(upstream)
            .is_some_and(rocky_compiler::semantic::ModelSchema::schema_is_complete);
        if !schema_complete {
            return Some(format!(
                "reads `{upstream}` through `SELECT *` and HEAD cannot enumerate `{upstream}`'s columns"
            ));
        }
    }

    let refs = match rocky_sql::lineage::all_column_references(sql) {
        Ok(r) => r,
        Err(e) => return Some(format!("lineage could not parse the HEAD SQL ({e})")),
    };
    // A reference qualified by a top-level alias that resolves to some
    // *other* physical relation is provably not the removed column. Anything
    // else with the column's name is a mention lineage did not attribute.
    let other_relation = |qualifier: &str| {
        lineage.source_tables.iter().any(|t| {
            t.binding == rocky_sql::lineage::TableBinding::Physical
                && t.name != "(subquery)"
                && !names_upstream(&t.name)
                && (t
                    .alias
                    .as_deref()
                    .is_some_and(|a| a.eq_ignore_ascii_case(qualifier))
                    || (t.alias.is_none() && t.name.eq_ignore_ascii_case(qualifier)))
        })
    };
    let ambiguous = refs.iter().find(|r| {
        r.column.eq_ignore_ascii_case(column) && !r.qualifier.as_deref().is_some_and(other_relation)
    })?;
    let spelled = match &ambiguous.qualifier {
        Some(q) => format!("{q}.{}", ambiguous.column),
        None => ambiguous.column.clone(),
    };
    Some(format!(
        "HEAD still mentions `{spelled}` where lineage cannot tell which relation it reads"
    ))
}

fn sorted(set: &BTreeSet<String>) -> Vec<String> {
    set.iter().cloned().collect()
}

/// Classify every direct consumer of a column removed from `upstream`.
///
/// Consumers come from the base graph (who read it before) and the HEAD graph
/// (who reads it now). Each is one of:
///
/// - `newly_broken` — HEAD still has a lineage edge from the removed column
///   into the consumer (or a new one);
/// - `deleted` — the consumer model is gone on HEAD;
/// - `repaired` — the consumer exists on HEAD and provably no longer reads
///   the column;
/// - `unknown` — HEAD did not compile, or the consumer mentions the column
///   somewhere lineage cannot attribute.
///
/// HEAD models that read `upstream` and mention the column ambiguously are
/// reported as `unknown` even when the base never read it, so a new silent
/// breakage is not dropped.
fn classify_removed_column_consumers(
    upstream: &str,
    column: &str,
    base: Option<&CompileResult>,
    head: Option<&CompileResult>,
) -> Vec<LineageConsumerImpact> {
    let base_reads = base
        .map(|b| direct_reads(&b.semantic_graph, upstream, column))
        .unwrap_or_default();
    let head_reads = head
        .map(|h| direct_reads(&h.semantic_graph, upstream, column))
        .unwrap_or_default();

    let mut consumers: BTreeSet<String> = base_reads.keys().cloned().collect();
    consumers.extend(head_reads.keys().cloned());
    if let Some(h) = head {
        for (name, schema) in &h.semantic_graph.models {
            if name != upstream && schema.upstream.iter().any(|u| u == upstream) {
                consumers.insert(name.clone());
            }
        }
    }

    let empty = Reads::default();
    let mut out = Vec::new();
    for consumer in consumers {
        let before = base_reads.get(&consumer).unwrap_or(&empty);
        let now = head_reads.get(&consumer).unwrap_or(&empty);
        let impact = |status, reads: &Reads, reason: String| LineageConsumerImpact {
            model: consumer.clone(),
            status,
            columns: sorted(&reads.columns),
            via: sorted(&reads.via),
            reason,
        };

        if !now.is_empty() {
            let reason = if before.is_empty() {
                format!("HEAD now reads `{upstream}.{column}`, which no longer exists")
            } else {
                format!("HEAD still reads `{upstream}.{column}`, which no longer exists")
            };
            out.push(impact(ConsumerImpactStatus::NewlyBroken, now, reason));
            continue;
        }
        let Some(head) = head else {
            out.push(impact(
                ConsumerImpactStatus::Unknown,
                before,
                "HEAD did not compile, so its reads cannot be checked".to_string(),
            ));
            continue;
        };
        let Some(head_model) = head.project.model(&consumer) else {
            out.push(impact(
                ConsumerImpactStatus::Deleted,
                before,
                "consumer model was removed on HEAD".to_string(),
            ));
            continue;
        };
        match unattributed_mention(&head_model.sql, upstream, column, &head.semantic_graph) {
            Some(reason) => out.push(impact(ConsumerImpactStatus::Unknown, before, reason)),
            None if !before.is_empty() => out.push(impact(
                ConsumerImpactStatus::Repaired,
                before,
                format!("HEAD no longer reads `{upstream}.{column}`"),
            )),
            // A HEAD model that reads the upstream but never read this
            // column: not a consumer.
            None => {}
        }
    }
    out.sort_by(|a, b| a.status.cmp(&b.status).then_with(|| a.model.cmp(&b.model)));
    out
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
        "**{} row(s) changed** ({} modified, {} added, {} removed, {} unchanged)\n\n",
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
                if c.change_type == ColumnChangeType::Removed && !c.consumer_impact.is_empty() {
                    format!("_removed; {}_", impact_summary(&c.consumer_impact))
                } else if c.change_type == ColumnChangeType::Removed {
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
        out.push('\n');
        write_consumer_impact_table(&mut out, &r.column_changes);
        out.push_str("</details>\n\n");
    }

    out
}

/// Per-consumer classification table for the removed columns of one model.
fn write_consumer_impact_table(out: &mut String, changes: &[LineageColumnChange]) {
    let rows: Vec<(&str, &LineageConsumerImpact)> = changes
        .iter()
        .flat_map(|c| {
            c.consumer_impact
                .iter()
                .map(move |i| (c.column_name.as_str(), i))
        })
        .collect();
    if rows.is_empty() {
        return;
    }
    out.push_str("**Consumers of removed columns**\n\n");
    out.push_str("| Removed column | Consumer | Status | Reads via | Detail |\n");
    out.push_str("|----------------|----------|--------|-----------|--------|\n");
    for (column, impact) in rows {
        let consumer = if impact.columns.is_empty() {
            format!("`{}`", impact.model)
        } else {
            format!(
                "`{}` ({})",
                impact.model,
                impact
                    .columns
                    .iter()
                    .map(|c| format!("`{c}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let status = match impact.status {
            ConsumerImpactStatus::NewlyBroken => "**newly broken**".to_string(),
            other => other.to_string(),
        };
        let via = if impact.via.is_empty() {
            "-".to_string()
        } else {
            impact.via.join(", ")
        };
        out.push_str(&format!(
            "| `{column}` | {consumer} | {status} | {via} | {} |\n",
            impact.reason
        ));
    }
    out.push('\n');
}

/// Short summary of a removed column's consumer classification for the
/// main table cell.
fn impact_summary(impacts: &[LineageConsumerImpact]) -> String {
    let mut counts: BTreeMap<ConsumerImpactStatus, usize> = BTreeMap::new();
    for i in impacts {
        *counts.entry(i.status).or_default() += 1;
    }
    counts
        .iter()
        .map(|(status, n)| format!("{n} {status}"))
        .collect::<Vec<_>>()
        .join(", ")
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
            resolved_target: None,
            status: ModelDiffStatus::Modified,
            row_count_before: None,
            row_count_after: None,
            column_changes: vec![col_change("amount", ColumnChangeType::TypeChanged)],
            sample_changed_rows: None,
        }];
        let enriched = enrich_with_downstream(&diff, None, None);
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
            resolved_target: None,
            status: ModelDiffStatus::Modified,
            row_count_before: None,
            row_count_after: None,
            column_changes: vec![col_change("legacy_flag", ColumnChangeType::Removed)],
            sample_changed_rows: None,
        }];
        // Pass None as a stand-in for head_compile (the compile_result wrap
        // is exercised in the integration test). The early-return on
        // `Removed` short-circuits before any graph access.
        let enriched = enrich_with_downstream(&diff, None, None);
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
                consumer_impact: vec![],
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
                consumer_impact: vec![],
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
                consumer_impact: vec![],
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

    // -----------------------------------------------------------------------
    // End-to-end: a real git repo, both refs compiled.
    // -----------------------------------------------------------------------

    use std::fs;
    use std::process::Command;

    use tempfile::TempDir;

    use crate::commands::ci_diff::compute_ci_diff_in;

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

    fn write_model(models: &Path, name: &str, sql: &str) {
        fs::write(models.join(format!("{name}.sql")), sql).unwrap();
        fs::write(
            models.join(format!("{name}.toml")),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"poc\"\nschema = \"marts\"\n",
        )
        .unwrap();
    }

    fn impacts_for(
        results: &[LineageDiffResult],
        model: &str,
        column: &str,
    ) -> Vec<LineageConsumerImpact> {
        results
            .iter()
            .filter(|r| r.model_name == model)
            .flat_map(|r| r.column_changes.iter())
            .filter(|c| c.column_name == column)
            .flat_map(|c| c.consumer_impact.clone())
            .collect()
    }

    /// G5: rename `stg_orders.amount` → `order_amount`. One consumer is
    /// repaired, one deleted, two newly broken (a value read and a filter
    /// read), one ambiguous. A consumer that reads `amount` from a different
    /// relation is not reported at all.
    #[test]
    fn e2e_renamed_column_classifies_each_consumer() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let models = dir.join("models");
        fs::create_dir_all(&models).unwrap();
        fs::write(
            dir.join("rocky.toml"),
            "[adapter]\ntype = \"duckdb\"\npath = \":memory:\"\n",
        )
        .unwrap();
        run_git(dir, &["init", "-q", "-b", "main"]);
        run_git(dir, &["config", "user.email", "tester@example.com"]);
        run_git(dir, &["config", "user.name", "Tester"]);
        run_git(dir, &["config", "commit.gpgsign", "false"]);

        write_model(
            &models,
            "stg_orders",
            "SELECT order_id, customer_id, amount FROM raw.orders",
        );
        write_model(
            &models,
            "fct_repaired",
            "SELECT order_id, amount FROM stg_orders",
        );
        write_model(
            &models,
            "fct_deleted",
            "SELECT order_id, amount AS deleted_amount FROM stg_orders",
        );
        write_model(
            &models,
            "fct_broken",
            "SELECT order_id, amount FROM stg_orders",
        );
        write_model(
            &models,
            "fct_filtered",
            "SELECT customer_id, COUNT(*) AS n FROM stg_orders WHERE amount > 0 GROUP BY customer_id",
        );
        write_model(
            &models,
            "fct_other_source",
            "SELECT s.order_id, r.amount AS raw_amount FROM stg_orders s \
             JOIN raw.orders r ON s.order_id = r.order_id",
        );
        write_model(
            &models,
            "fct_ambiguous",
            "SELECT o.order_id, amount FROM stg_orders o \
             JOIN raw.customers c ON o.customer_id = c.customer_id",
        );
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "base"]);

        // HEAD: rename the upstream column, repair one consumer (keeping its
        // own output name), delete another, leave the rest untouched.
        write_model(
            &models,
            "stg_orders",
            "SELECT order_id, customer_id, amount AS order_amount FROM raw.orders",
        );
        write_model(
            &models,
            "fct_repaired",
            "SELECT order_id, order_amount AS amount FROM stg_orders",
        );
        fs::remove_file(models.join("fct_deleted.sql")).unwrap();
        fs::remove_file(models.join("fct_deleted.toml")).unwrap();
        run_git(dir, &["add", "-A"]);
        run_git(dir, &["commit", "-q", "-m", "rename amount"]);

        let data = compute_ci_diff_in(
            &dir.join("rocky.toml"),
            &dir.join("state.redb"),
            "HEAD~1",
            &models,
            None,
            CiDiffMode::Head,
            Some(dir),
        )
        .expect("compute_ci_diff must succeed");
        assert!(data.base_compile.is_some(), "base must compile");
        assert!(data.head_compile.is_some(), "head must compile");

        let results = enrich_with_downstream(
            &data.results,
            data.head_compile.as_ref(),
            data.base_compile.as_ref(),
        );
        let impacts = impacts_for(&results, "stg_orders", "amount");
        let status: std::collections::HashMap<&str, ConsumerImpactStatus> = impacts
            .iter()
            .map(|i| (i.model.as_str(), i.status))
            .collect();

        assert_eq!(
            status.get("fct_repaired"),
            Some(&ConsumerImpactStatus::Repaired),
            "{impacts:#?}"
        );
        assert_eq!(
            status.get("fct_deleted"),
            Some(&ConsumerImpactStatus::Deleted),
            "{impacts:#?}"
        );
        assert_eq!(
            status.get("fct_broken"),
            Some(&ConsumerImpactStatus::NewlyBroken),
            "{impacts:#?}"
        );
        assert_eq!(
            status.get("fct_filtered"),
            Some(&ConsumerImpactStatus::NewlyBroken),
            "{impacts:#?}"
        );
        assert_eq!(
            status.get("fct_ambiguous"),
            Some(&ConsumerImpactStatus::Unknown),
            "{impacts:#?}"
        );
        assert!(
            !status.contains_key("fct_other_source"),
            "a read of raw.orders.amount is not a read of stg_orders.amount: {impacts:#?}"
        );

        let filtered = impacts.iter().find(|i| i.model == "fct_filtered").unwrap();
        assert_eq!(filtered.via, vec!["filter".to_string()]);
        let broken = impacts.iter().find(|i| i.model == "fct_broken").unwrap();
        assert_eq!(broken.via, vec!["value".to_string()]);
        assert_eq!(broken.columns, vec!["amount".to_string()]);

        // The renamed-to column is an addition with no consumer impact.
        assert!(impacts_for(&results, "stg_orders", "order_amount").is_empty());

        let md = format_lineage_diff_markdown(&results, &data.summary);
        assert!(md.contains("Consumers of removed columns"), "{md}");
        assert!(md.contains("**newly broken**"), "{md}");
        assert!(
            md.contains("| `amount` | `fct_repaired` (`amount`) | repaired |"),
            "{md}"
        );
        assert!(
            md.contains("`fct_deleted` (`deleted_amount`) | deleted |"),
            "{md}"
        );
    }
}
