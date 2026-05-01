//! `rocky archive` — archive old data partitions.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Result;

use crate::output::{ArchiveOutput, ArchiveTableEntry, ArchiveTotals, NamedStatement, print_json};
use crate::scope::resolve_managed_tables_in_catalog;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky archive`.
pub fn run_archive(
    model: Option<&str>,
    older_than: &str,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    // Parse the older_than duration (e.g., "90d", "6m", "1y")
    let days = parse_duration_days(older_than)?;
    let statements = generate_archive_sql(model, days);

    if output_json {
        let typed_statements: Vec<NamedStatement> = statements
            .iter()
            .map(|(purpose, sql)| NamedStatement {
                purpose: purpose.clone(),
                sql: sql.clone(),
            })
            .collect();
        let output = ArchiveOutput {
            version: VERSION.to_string(),
            command: "archive".to_string(),
            model: model.map(String::from),
            catalog: None,
            scope: None,
            older_than: older_than.to_string(),
            older_than_days: days,
            dry_run,
            statements: typed_statements,
            tables: None,
            totals: None,
        };
        print_json(&output)?;
    } else {
        println!(
            "Archive plan: data older than {older_than} ({days} days){}",
            if dry_run { " [DRY RUN]" } else { "" }
        );
        if let Some(m) = model {
            println!("Model: {m}");
        }
        println!();

        for (purpose, sql) in &statements {
            println!("-- {purpose}");
            println!("{sql};");
            println!();
        }

        if dry_run {
            println!("(dry run: no statements executed)");
        }
    }

    Ok(())
}

/// Execute `rocky archive --catalog <name>` — generate archive SQL for
/// every Rocky-managed table in the catalog.
///
/// Mirrors `rocky compact --catalog`: resolves the managed-table set via
/// [`resolve_managed_tables_in_catalog`] (config-only, no warehouse round
/// trip), then templates `DELETE` / `VACUUM` per table and aggregates
/// into a single envelope.
pub async fn run_archive_catalog(
    config_path: &Path,
    catalog: &str,
    older_than: &str,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    let days = parse_duration_days(older_than)?;
    let scope = resolve_managed_tables_in_catalog(config_path, catalog).await?;

    let mut flat_statements: Vec<NamedStatement> = Vec::new();
    let mut per_table: BTreeMap<String, ArchiveTableEntry> = BTreeMap::new();

    for fqn in &scope.tables {
        let stmts = generate_archive_sql(Some(fqn), days);
        let typed: Vec<NamedStatement> = stmts
            .into_iter()
            .map(|(purpose, sql)| NamedStatement { purpose, sql })
            .collect();
        flat_statements.extend(typed.iter().cloned());
        per_table.insert(fqn.clone(), ArchiveTableEntry { statements: typed });
    }

    let totals = ArchiveTotals {
        table_count: scope.tables.len(),
        statement_count: flat_statements.len(),
    };

    if output_json {
        let output = ArchiveOutput {
            version: VERSION.to_string(),
            command: "archive".to_string(),
            model: None,
            catalog: Some(scope.catalog.clone()),
            scope: Some("catalog".to_string()),
            older_than: older_than.to_string(),
            older_than_days: days,
            dry_run,
            statements: flat_statements,
            tables: Some(per_table),
            totals: Some(totals),
        };
        print_json(&output)?;
    } else {
        println!(
            "Archive plan for catalog '{}' ({} tables, older than {} = {} days){}",
            scope.catalog,
            scope.tables.len(),
            older_than,
            days,
            if dry_run { " [DRY RUN]" } else { "" }
        );
        println!();

        for (fqn, entry) in &per_table {
            println!("-- {fqn}");
            for s in &entry.statements {
                println!("-- {}", s.purpose);
                println!("{};", s.sql);
            }
            println!();
        }

        if dry_run {
            println!("(dry run: no statements executed)");
        }
    }

    Ok(())
}

/// Parses a human-readable duration into days.
///
/// Supported formats: `30d`, `6m`, `1y`, `90` (days).
fn parse_duration_days(s: &str) -> Result<u64> {
    let s = s.trim();
    if let Some(n) = s.strip_suffix('d') {
        n.parse::<u64>()
            .map_err(|e| anyhow::anyhow!("invalid duration: {e}"))
    } else if let Some(n) = s.strip_suffix('m') {
        let months: u64 = n
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid duration: {e}"))?;
        Ok(months * 30)
    } else if let Some(n) = s.strip_suffix('y') {
        let years: u64 = n
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid duration: {e}"))?;
        Ok(years * 365)
    } else {
        s.parse::<u64>()
            .map_err(|e| anyhow::anyhow!("invalid duration '{s}': {e} (use 30d, 6m, or 1y)"))
    }
}

/// Generates archive SQL (DELETE + VACUUM) for old data.
fn generate_archive_sql(model: Option<&str>, days: u64) -> Vec<(String, String)> {
    let mut stmts = Vec::new();
    let target = model.unwrap_or("*");

    // Delete old rows
    stmts.push((
        format!("delete rows older than {days} days"),
        format!(
            "DELETE FROM {target}\n\
             WHERE _fivetran_synced < DATEADD(DAY, -{days}, CURRENT_TIMESTAMP())"
        ),
    ));

    // VACUUM after deletion to reclaim space
    stmts.push((
        "reclaim storage after deletion".to_string(),
        format!("VACUUM {target} RETAIN 0 HOURS"),
    ));

    stmts
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration_days("30d").unwrap(), 30);
        assert_eq!(parse_duration_days("90").unwrap(), 90);
        assert_eq!(parse_duration_days("6m").unwrap(), 180);
        assert_eq!(parse_duration_days("1y").unwrap(), 365);
        assert_eq!(parse_duration_days("2y").unwrap(), 730);
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration_days("abc").is_err());
        assert!(parse_duration_days("").is_err());
    }

    #[test]
    fn test_generate_archive_sql() {
        let stmts = generate_archive_sql(Some("catalog.schema.orders"), 90);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].1.contains("DELETE FROM"));
        assert!(stmts[0].1.contains("-90"));
        assert!(stmts[1].1.contains("VACUUM"));
    }

    #[test]
    fn test_generate_archive_sql_wildcard() {
        let stmts = generate_archive_sql(None, 30);
        assert!(stmts[0].1.contains("DELETE FROM *"));
    }
}
