//! `rocky shell` — interactive SQL REPL against the configured warehouse.

use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::debug;

use rocky_core::traits::WarehouseAdapter;

use crate::registry;

/// Execute `rocky shell` — interactive SQL REPL.
///
/// Loads config, resolves the warehouse adapter (from the pipeline's adapter
/// reference or the default adapter), prints a welcome banner, and enters a
/// read-eval-print loop that sends each SQL statement to the warehouse via
/// `adapter.execute_query()`.
///
/// Special dot-commands:
/// - `.quit` / `.exit` — exit the REPL
/// - `.tables` — list tables in the current schema
/// - `.schema <table>` — describe a table's columns
///
/// Multi-line input: lines ending with `\` are continued on the next line.
/// Ctrl-D (EOF) and Ctrl-C exit gracefully.
pub async fn run_shell(config_path: &Path, pipeline_name: Option<&str>) -> Result<()> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;

    let adapter_registry = registry::AdapterRegistry::from_config(&rocky_cfg)?;

    // Resolve adapter: use the pipeline's adapter ref if a pipeline is specified,
    // otherwise use the first (or only) warehouse adapter.
    let (adapter_name, adapter) =
        resolve_shell_adapter(&rocky_cfg, &adapter_registry, pipeline_name)?;

    // Determine adapter type for the banner
    let adapter_type = rocky_cfg
        .adapters
        .get(&adapter_name)
        .map(|c| c.adapter_type.as_str())
        .unwrap_or("unknown");

    // Verify connectivity
    eprint!("Connecting... ");
    io::stderr().flush().ok();
    adapter
        .ping()
        .await
        .context("failed to connect to warehouse")?;
    eprintln!("ok");

    print_banner(adapter_type, &adapter_name);

    repl_loop(adapter).await
}

/// Resolve which warehouse adapter to use for the shell.
///
/// Priority:
/// 1. If `pipeline_name` is given, use that pipeline's target adapter.
/// 2. If there is exactly one pipeline, use its target adapter.
/// 3. If there is exactly one warehouse adapter, use it directly.
/// 4. Otherwise, error with available options.
fn resolve_shell_adapter(
    config: &rocky_core::config::RockyConfig,
    registry: &registry::AdapterRegistry,
    pipeline_name: Option<&str>,
) -> Result<(String, Arc<dyn WarehouseAdapter>)> {
    // Try pipeline-based resolution first
    if pipeline_name.is_some() || config.pipelines.len() == 1 {
        if let Ok((_, pipeline)) = registry::resolve_pipeline(config, pipeline_name) {
            let adapter_ref = pipeline_target_adapter(pipeline);
            let adapter = registry.warehouse_adapter(&adapter_ref)?;
            return Ok((adapter_ref, adapter));
        }
    }

    // Fallback: use the only warehouse adapter
    let names = registry.warehouse_adapter_names();
    match names.len() {
        0 => anyhow::bail!(
            "no warehouse adapters configured. Add an [adapter] section to rocky.toml."
        ),
        1 => {
            let name = &names[0];
            let adapter = registry.warehouse_adapter(name)?;
            Ok((name.clone(), adapter))
        }
        _ => {
            anyhow::bail!(
                "multiple warehouse adapters configured ({}). Use --pipeline <name> to select one.",
                names.join(", ")
            );
        }
    }
}

/// Extract the target adapter name from any pipeline variant.
fn pipeline_target_adapter(pipeline: &rocky_core::config::PipelineConfig) -> String {
    match pipeline {
        rocky_core::config::PipelineConfig::Replication(r) => r.target.adapter.clone(),
        rocky_core::config::PipelineConfig::Transformation(t) => t.target.adapter.clone(),
        rocky_core::config::PipelineConfig::Quality(q) => q.target.adapter.clone(),
        rocky_core::config::PipelineConfig::Snapshot(s) => s.target.adapter.clone(),
        rocky_core::config::PipelineConfig::Load(l) => l.target.adapter.clone(),
    }
}

fn print_banner(adapter_type: &str, adapter_name: &str) {
    eprintln!();
    eprintln!("Rocky Shell (adapter: {adapter_type}, name: {adapter_name})");
    eprintln!("Type SQL to execute. Special commands:");
    eprintln!("  .tables              List tables in a schema (.tables catalog.schema)");
    eprintln!("  .schema <table>      Describe a table's columns");
    eprintln!("  .quit / .exit        Exit the shell");
    eprintln!("  Lines ending with \\ continue on the next line.");
    eprintln!();
}

/// Main REPL loop.
async fn repl_loop(adapter: Arc<dyn WarehouseAdapter>) -> Result<()> {
    let stdin = io::stdin();
    let mut reader = stdin.lock();
    let mut line_buf = String::new();
    let mut accumulator = String::new();

    loop {
        // Print prompt
        let prompt = if accumulator.is_empty() {
            "rocky> "
        } else {
            "   ... "
        };
        eprint!("{prompt}");
        io::stderr().flush().ok();

        line_buf.clear();
        let bytes_read = reader.read_line(&mut line_buf)?;

        // EOF (Ctrl-D)
        if bytes_read == 0 {
            if !accumulator.is_empty() {
                eprintln!();
                eprintln!("Incomplete input discarded.");
            }
            eprintln!("Bye.");
            return Ok(());
        }

        let trimmed = line_buf.trim_end_matches('\n').trim_end_matches('\r');

        // Handle line continuation (trailing backslash)
        if let Some(prefix) = trimmed.strip_suffix('\\') {
            accumulator.push_str(prefix);
            accumulator.push(' ');
            continue;
        }

        // Append to accumulator
        if accumulator.is_empty() {
            accumulator = trimmed.to_string();
        } else {
            accumulator.push_str(trimmed);
        }

        let input = accumulator.trim().to_string();
        accumulator.clear();

        if input.is_empty() {
            continue;
        }

        // Dot-commands
        if input.starts_with('.') {
            match handle_dot_command(&input, &adapter).await {
                DotResult::Continue => continue,
                DotResult::Quit => {
                    eprintln!("Bye.");
                    return Ok(());
                }
            }
        }

        // Strip trailing semicolons (common habit from psql/mysql)
        let sql = input.trim_end_matches(';').trim();
        if sql.is_empty() {
            continue;
        }

        // Execute SQL
        debug!(sql, "shell: executing query");
        match adapter.execute_query(sql).await {
            Ok(result) => {
                print_query_result(&result);
            }
            Err(e) => {
                eprintln!("Error: {e}");
            }
        }
    }
}

enum DotResult {
    Continue,
    Quit,
}

async fn handle_dot_command(input: &str, adapter: &Arc<dyn WarehouseAdapter>) -> DotResult {
    let parts: Vec<&str> = input.splitn(2, char::is_whitespace).collect();
    let cmd = parts[0].to_lowercase();
    let arg = parts.get(1).map(|s| s.trim()).unwrap_or("");

    match cmd.as_str() {
        ".quit" | ".exit" => DotResult::Quit,
        ".tables" => {
            let sql = if arg.is_empty() {
                "SHOW TABLES".to_string()
            } else {
                // arg might be "catalog.schema" or just "schema"
                format!("SHOW TABLES IN {arg}")
            };
            match adapter.execute_query(&sql).await {
                Ok(result) => print_query_result(&result),
                Err(e) => eprintln!("Error: {e}"),
            }
            DotResult::Continue
        }
        ".schema" => {
            if arg.is_empty() {
                eprintln!("Usage: .schema <table>");
            } else {
                let sql = format!("DESCRIBE TABLE {arg}");
                match adapter.execute_query(&sql).await {
                    Ok(result) => print_query_result(&result),
                    Err(e) => eprintln!("Error: {e}"),
                }
            }
            DotResult::Continue
        }
        _ => {
            eprintln!("Unknown command: {cmd}");
            eprintln!("Available: .tables, .schema <table>, .quit, .exit");
            DotResult::Continue
        }
    }
}

/// Render a `QueryResult` as a column-aligned table to stdout.
fn print_query_result(result: &rocky_core::traits::QueryResult) {
    if result.columns.is_empty() {
        println!("(no columns)");
        return;
    }

    if result.rows.is_empty() {
        println!("(0 rows)");
        return;
    }

    // Compute column widths: max of header length and cell contents.
    let num_cols = result.columns.len();
    let mut widths: Vec<usize> = result
        .columns
        .iter()
        .map(std::string::String::len)
        .collect();

    let formatted_rows: Vec<Vec<String>> = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, val)| {
                    let s = format_value(val);
                    if i < num_cols {
                        widths[i] = widths[i].max(s.len());
                    }
                    s
                })
                .collect()
        })
        .collect();

    // Cap column widths at a reasonable max to avoid blowout
    let max_col_width = 60;
    for w in &mut widths {
        if *w > max_col_width {
            *w = max_col_width;
        }
    }

    // Print header
    let header: Vec<String> = result
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let w = widths.get(i).copied().unwrap_or(c.len());
            format!("{:<width$}", truncate(c, max_col_width), width = w)
        })
        .collect();
    println!(" {}", header.join(" | "));

    // Print separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    println!("-{}-", sep.join("-+-"));

    // Print rows
    for row in &formatted_rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, val)| {
                let w = widths.get(i).copied().unwrap_or(val.len());
                format!("{:<width$}", truncate(val, max_col_width), width = w)
            })
            .collect();
        println!(" {}", cells.join(" | "));
    }

    println!("({} rows)", result.rows.len());
}

/// Format a JSON value for display.
fn format_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => val.to_string(),
    }
}

/// Truncate a string to `max` characters, appending "..." if truncated.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else if max > 3 {
        format!("{}...", &s[..max - 3])
    } else {
        s[..max].to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_value_null() {
        assert_eq!(format_value(&serde_json::Value::Null), "NULL");
    }

    #[test]
    fn test_format_value_string() {
        let v = serde_json::Value::String("hello".into());
        assert_eq!(format_value(&v), "hello");
    }

    #[test]
    fn test_format_value_number() {
        let v = serde_json::json!(42);
        assert_eq!(format_value(&v), "42");
    }

    #[test]
    fn test_format_value_bool() {
        assert_eq!(format_value(&serde_json::json!(true)), "true");
    }

    #[test]
    fn test_format_value_array() {
        let v = serde_json::json!([1, 2, 3]);
        assert_eq!(format_value(&v), "[1,2,3]");
    }

    #[test]
    fn test_truncate_short() {
        assert_eq!(truncate("abc", 10), "abc");
    }

    #[test]
    fn test_truncate_exact() {
        assert_eq!(truncate("abcde", 5), "abcde");
    }

    #[test]
    fn test_truncate_long() {
        assert_eq!(truncate("abcdefghij", 7), "abcd...");
    }

    #[test]
    fn test_print_empty_result() {
        let result = rocky_core::traits::QueryResult {
            columns: vec!["a".into()],
            rows: vec![],
        };
        // Should not panic
        print_query_result(&result);
    }

    #[test]
    fn test_print_no_columns() {
        let result = rocky_core::traits::QueryResult {
            columns: vec![],
            rows: vec![],
        };
        print_query_result(&result);
    }

    #[test]
    fn test_print_simple_result() {
        let result = rocky_core::traits::QueryResult {
            columns: vec!["name".into(), "age".into()],
            rows: vec![
                vec![serde_json::json!("Alice"), serde_json::json!(30)],
                vec![serde_json::json!("Bob"), serde_json::json!(25)],
            ],
        };
        // Should not panic; output goes to stdout
        print_query_result(&result);
    }
}
