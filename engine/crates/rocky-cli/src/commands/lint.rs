//! `rocky lint` — SQL style lint for model files.
//!
//! Reads `.sql` files, runs the rules in [`rocky_sql::style_lint`], applies
//! the `[lint]` section of `rocky.toml` (rules to disable, severity
//! overrides) and reports `file:line:col` findings. `--fix` rewrites the
//! mechanical rules in place. The command exits non-zero when a finding has
//! `error` severity.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rocky_core::config::{LintConfig, LintSeverity};
use rocky_sql::style_lint::{self, RuleInfo, StyleFinding};

use crate::output::{LintCounts, LintFinding, LintOutput, print_json};

/// Severity a rule has when `[lint.severity]` does not override it.
fn default_severity(code: &str) -> LintSeverity {
    match code {
        "S003" | "S004" => LintSeverity::Info,
        _ => LintSeverity::Warning,
    }
}

/// `[lint]` resolved against the rule table.
struct Settings {
    disabled: HashSet<&'static str>,
    severity: BTreeMap<&'static str, LintSeverity>,
}

impl Settings {
    /// Resolve `[lint]`. An unknown rule code is an error, so a typo never
    /// silently leaves a rule on.
    fn resolve(config: &LintConfig) -> Result<Self> {
        let known = |code: &str, key: &str| -> Result<&'static RuleInfo> {
            style_lint::rule_info(code).with_context(|| {
                let codes: Vec<&str> = style_lint::RULES.iter().map(|r| r.code).collect();
                format!(
                    "[lint] {key}: unknown rule `{code}` (known rules: {})",
                    codes.join(", ")
                )
            })
        };
        let mut disabled = HashSet::new();
        for code in &config.disable {
            disabled.insert(known(code, "disable")?.code);
        }
        let mut severity = BTreeMap::new();
        for (code, level) in &config.severity {
            severity.insert(known(code, "severity")?.code, *level);
        }
        Ok(Self { disabled, severity })
    }

    fn enabled(&self, code: &str) -> bool {
        !self.disabled.contains(code)
    }

    fn severity_of(&self, code: &str) -> LintSeverity {
        self.severity
            .get(code)
            .copied()
            .unwrap_or_else(|| default_severity(code))
    }
}

/// Lint one SQL text. Returns the enabled findings and whether the AST rules
/// were skipped because the SQL did not parse.
fn lint_text(sql: &str, settings: &Settings) -> (Vec<StyleFinding>, bool) {
    let report = style_lint::lint_style(sql);
    let findings = report
        .findings
        .into_iter()
        .filter(|f| settings.enabled(f.code))
        .collect();
    (findings, report.ast_rules_skipped)
}

/// Execute `rocky lint`.
///
/// `paths` are `.sql` files or directories (walked recursively). With no
/// paths the `models` directory is linted.
pub fn run_lint(config_path: &Path, paths: &[PathBuf], fix: bool, json: bool) -> Result<()> {
    let config = rocky_core::config::load_optional_project_config(Some(config_path))
        .with_context(|| format!("loading {}", config_path.display()))?;
    let settings = Settings::resolve(&config.map(|c| c.lint).unwrap_or_default())?;

    let output = lint_paths(paths, fix, &settings)?;

    if json {
        print_json(&output)?;
    } else {
        render_text(&output, fix);
    }

    if output.counts.error > 0 {
        std::process::exit(1);
    }
    Ok(())
}

fn lint_paths(paths: &[PathBuf], fix: bool, settings: &Settings) -> Result<LintOutput> {
    let default_root = [PathBuf::from("models")];
    let roots: &[PathBuf] = if paths.is_empty() {
        &default_root
    } else {
        paths
    };
    let files = collect_sql_files(roots)?;

    let mut findings = Vec::new();
    let mut counts = LintCounts::default();
    let mut fixed = 0;
    let mut files_fixed = Vec::new();
    let mut ast_rules_skipped = Vec::new();

    for file in &files {
        let display = file.display().to_string();
        let mut sql =
            std::fs::read_to_string(file).with_context(|| format!("reading {display}"))?;

        if fix {
            let (fixed_sql, n) = style_lint::fix_style(&sql, &|code| settings.enabled(code));
            if n > 0 && fixed_sql != sql {
                // A fix must not break a file the parser could read.
                let was_parsable = !style_lint::lint_style(&sql).ast_rules_skipped;
                let still_parsable = !style_lint::lint_style(&fixed_sql).ast_rules_skipped;
                if was_parsable && !still_parsable {
                    eprintln!("skipped fix: {display} would stop parsing");
                } else {
                    std::fs::write(file, &fixed_sql)
                        .with_context(|| format!("writing {display}"))?;
                    fixed += n;
                    files_fixed.push(display.clone());
                    sql = fixed_sql;
                }
            }
        }

        let (file_findings, skipped) = lint_text(&sql, settings);
        if skipped {
            ast_rules_skipped.push(display.clone());
        }
        for f in file_findings {
            let severity = settings.severity_of(f.code);
            match severity {
                LintSeverity::Error => counts.error += 1,
                LintSeverity::Warning => counts.warning += 1,
                LintSeverity::Info => counts.info += 1,
            }
            let info = style_lint::rule_info(f.code);
            findings.push(LintFinding {
                code: f.code.to_string(),
                rule: info.map_or_else(String::new, |r| r.name.to_string()),
                severity,
                file: display.clone(),
                line: f.line,
                col: f.col,
                message: f.message,
                hint: f.hint,
                fixable: info.is_some_and(|r| r.fixable),
            });
        }
    }

    Ok(LintOutput {
        findings,
        counts,
        fixed,
        files_fixed,
        ast_rules_skipped,
        ..LintOutput::new(files.len())
    })
}

fn render_text(out: &LintOutput, fix: bool) {
    for f in &out.findings {
        let level = match f.severity {
            LintSeverity::Error => "error",
            LintSeverity::Warning => "warning",
            LintSeverity::Info => "info",
        };
        println!(
            "{}:{}:{}: {level}[{}] {}",
            f.file, f.line, f.col, f.code, f.message
        );
        println!("    hint: {}", f.hint);
    }
    for file in &out.ast_rules_skipped {
        eprintln!("note: {file} did not parse; S001, S003 and S004 were skipped");
    }
    if fix {
        println!(
            "fixed {} finding(s) in {} file(s)",
            out.fixed,
            out.files_fixed.len()
        );
    }
    let fixable = out.findings.iter().filter(|f| f.fixable).count();
    println!(
        "{} file(s) checked: {} error(s), {} warning(s), {} info",
        out.files_checked, out.counts.error, out.counts.warning, out.counts.info
    );
    if fixable > 0 && !fix {
        println!("{fixable} finding(s) can be fixed with `rocky lint --fix`");
    }
}

// ---------------------------------------------------------------------------
// File discovery
// ---------------------------------------------------------------------------

fn collect_sql_files(roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for root in roots {
        if root.is_file() {
            if root.extension().is_some_and(|e| e == "sql") {
                files.push(root.clone());
            }
        } else if root.is_dir() {
            walk_dir(root, &mut files)?;
        } else {
            bail!("path does not exist: {}", root.display());
        }
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn walk_dir(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if path.is_dir() {
            if !name.starts_with('.') && name != "target" && name != "node_modules" {
                walk_dir(&path, out)?;
            }
        } else if path.extension().is_some_and(|e| e == "sql") {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> Settings {
        Settings::resolve(&LintConfig::default()).unwrap()
    }

    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn reports_position_severity_and_counts() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "m.sql",
            "SELECT a.x, y FROM a JOIN b ON a.k = b.k  \n",
        );
        let out = lint_paths(&[dir.path().to_path_buf()], false, &defaults()).unwrap();
        let codes: Vec<&str> = out.findings.iter().map(|f| f.code.as_str()).collect();
        assert_eq!(codes, ["S001", "S002", "S006"]);
        assert_eq!(out.counts.warning, 3);
        assert_eq!(out.counts.error, 0);
        assert!(out.findings.iter().all(|f| f.line == 1 && f.col > 0));
        assert!(out.findings.iter().any(|f| f.fixable));
        assert_eq!(out.files_checked, 1);
    }

    #[test]
    fn disable_and_severity_come_from_config() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "m.sql", "SELECT * FROM t  \n");
        let config: LintConfig = toml::from_str(
            r#"
            disable = ["s006"]
            [severity]
            S003 = "error"
            "#,
        )
        .unwrap();
        let settings = Settings::resolve(&config).unwrap();
        let out = lint_paths(&[dir.path().to_path_buf()], false, &settings).unwrap();
        assert_eq!(out.findings.len(), 1);
        assert_eq!(out.findings[0].code, "S003");
        assert_eq!(out.findings[0].severity, LintSeverity::Error);
        assert_eq!(out.counts.error, 1);
    }

    #[test]
    fn unknown_rule_in_config_is_an_error() {
        let config: LintConfig = toml::from_str(r#"disable = ["S999"]"#).unwrap();
        let err = Settings::resolve(&config).err().unwrap().to_string();
        assert!(err.contains("S999"), "{err}");
        let config: LintConfig = toml::from_str("[severity]\nX1 = \"error\"").unwrap();
        assert!(Settings::resolve(&config).is_err());
    }

    #[test]
    fn fix_rewrites_mechanical_rules_and_keeps_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            "m.sql",
            "select a.x, b.y\nFROM a JOIN b ON a.k = b.k  \n",
        );
        let out = lint_paths(std::slice::from_ref(&p), true, &defaults()).unwrap();
        assert_eq!(
            std::fs::read_to_string(&p).unwrap(),
            "SELECT a.x, b.y\nFROM a INNER JOIN b ON a.k = b.k\n"
        );
        assert_eq!(out.fixed, 3);
        assert_eq!(out.files_fixed.len(), 1);
        assert!(out.findings.is_empty());
    }

    #[test]
    fn fix_leaves_report_only_findings_and_disabled_rules() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            "m.sql",
            "SELECT * FROM a JOIN b ON a.k = b.k \n",
        );
        let config: LintConfig = toml::from_str(r#"disable = ["S002"]"#).unwrap();
        let settings = Settings::resolve(&config).unwrap();
        let out = lint_paths(std::slice::from_ref(&p), true, &settings).unwrap();
        assert_eq!(
            std::fs::read_to_string(&p).unwrap(),
            "SELECT * FROM a JOIN b ON a.k = b.k\n"
        );
        let codes: Vec<&str> = out.findings.iter().map(|f| f.code.as_str()).collect();
        assert_eq!(codes, ["S003"]);
    }

    #[test]
    fn clean_file_has_no_findings_and_is_not_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let sql =
            "SELECT o.id, i.amount\nFROM orders AS o\nINNER JOIN items AS i ON o.id = i.order_id\n";
        let p = write(dir.path(), "m.sql", sql);
        let out = lint_paths(std::slice::from_ref(&p), true, &defaults()).unwrap();
        assert!(out.findings.is_empty());
        assert_eq!(out.fixed, 0);
        assert_eq!(std::fs::read_to_string(&p).unwrap(), sql);
    }

    #[test]
    fn unparsable_file_still_gets_text_rules() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "m.sql", "SELECT FROM WHERE (  \n");
        let out = lint_paths(&[dir.path().to_path_buf()], false, &defaults()).unwrap();
        assert_eq!(out.ast_rules_skipped.len(), 1);
        assert!(out.findings.iter().any(|f| f.code == "S006"));
    }

    #[test]
    fn missing_path_is_an_error_and_non_sql_is_ignored() {
        assert!(lint_paths(&[PathBuf::from("/nonexistent/models")], false, &defaults()).is_err());
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "m.rocky", "from t  \n");
        write(dir.path(), "m.toml", "name = 'x'  \n");
        let out = lint_paths(&[dir.path().to_path_buf()], false, &defaults()).unwrap();
        assert_eq!(out.files_checked, 0);
    }

    /// Control: the playground models read their columns through aliases, so
    /// `S001` must stay silent on them. Every `S002` must sit on a real bare
    /// `JOIN`, and `--fix` must leave every file parsable and clean.
    #[test]
    fn playground_models_have_no_false_positives() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../examples/playground");
        let mut files = Vec::new();
        collect_models(&root, &mut files);
        assert!(files.len() > 10, "found only {} model files", files.len());
        let settings = defaults();
        let mut bad = Vec::new();
        for f in &files {
            let sql = std::fs::read_to_string(f).unwrap();
            let (findings, skipped) = lint_text(&sql, &settings);
            for finding in &findings {
                let line = sql.lines().nth(finding.line as usize - 1).unwrap_or("");
                let at: String = line.chars().skip(finding.col as usize - 1).collect();
                let ok = match finding.code {
                    "S001" => false,
                    "S002" => at.to_ascii_lowercase().starts_with("join"),
                    _ => true,
                };
                if !ok {
                    bad.push(format!(
                        "{}:{}:{} {}",
                        f.display(),
                        finding.line,
                        finding.col,
                        finding.code
                    ));
                }
            }
            let (fixed, _) = style_lint::fix_style(&sql, &|c| settings.enabled(c));
            let after = style_lint::lint_style(&fixed);
            if after.ast_rules_skipped != skipped
                || after.findings.iter().any(|f| !f.fix.is_empty())
            {
                bad.push(format!("{}: fix is not clean", f.display()));
            }
        }
        assert!(bad.is_empty(), "unexpected findings:\n{}", bad.join("\n"));
    }

    fn collect_models(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name == "target" || name.starts_with('.') {
                    continue;
                }
                if name == "models" {
                    let _ = walk_dir(&p, out);
                } else {
                    collect_models(&p, out);
                }
            }
        }
    }
}
