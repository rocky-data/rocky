//! RV-E2 throwaway harness (uncommitted). Runs the X1 grain spike over a
//! directory of `rocky import-dbt` model files.
//!
//! usage: grain_scan <imported_models_dir> <dbt_project_dir> <model,model,...>
//!
//! Output: one TSV line per target model:
//! model  parse  variant  n_g001  grain  messages
//! variant A = prototype grain sources only (structural inference + merge unique_key)
//! variant B = A plus dbt `unique` tests (declared, tested) as upstream grain
//! variant C = B plus positional GROUP BY resolved (secondary headroom only)

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use regex::Regex;
use rocky_compiler::grain::{self, DeclaredKeyKind, Grain};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let models_dir = Path::new(&args[1]);
    let dbt_dir = Path::new(&args[2]);
    let targets: Vec<String> = args[3].split(',').map(|s| s.trim().to_lowercase()).collect();

    // dbt var -> ref target (e.g. invoice: "{{ ref('stg_stripe__invoice') }}")
    let mut var_map: HashMap<String, String> = HashMap::new();
    if let Ok(yml) = std::fs::read_to_string(dbt_dir.join("dbt_project.yml")) {
        let re = Regex::new(r#"(?m)^\s*([A-Za-z0-9_]+)\s*:\s*['"]\{\{\s*(?:ref|source)\(\s*['"]([^'"]+)['"](?:\s*,\s*['"]([^'"]+)['"])?\s*\)\s*\}\}['"]"#).unwrap();
        for c in re.captures_iter(&yml) {
            let target = c.get(3).or(c.get(2)).unwrap().as_str().to_lowercase();
            var_map.insert(c[1].to_lowercase(), target);
        }
    }

    let mut sqls: HashMap<String, String> = HashMap::new();
    let mut uniques: HashMap<String, Vec<BTreeSet<String>>> = HashMap::new();
    let mut merge_keys: HashMap<String, Vec<String>> = HashMap::new();
    for e in std::fs::read_dir(models_dir).unwrap().flatten() {
        let p = e.path();
        let stem = p.file_stem().unwrap().to_string_lossy().to_lowercase();
        match p.extension().and_then(|x| x.to_str()) {
            Some("sql") => {
                let raw = std::fs::read_to_string(&p).unwrap();
                sqls.insert(stem, preprocess(&raw, &var_map));
            }
            Some("toml") => {
                let raw = std::fs::read_to_string(&p).unwrap();
                let Ok(t) = toml::from_str::<toml::Table>(&raw) else { continue }; let v = toml::Value::Table(t);
                if let Some(k) = v.get("strategy").and_then(|s| s.get("unique_key")) {
                    let keys: Vec<String> = match k {
                        toml::Value::String(s) => vec![s.to_lowercase()],
                        toml::Value::Array(a) => a.iter().filter_map(|x| x.as_str()).map(str::to_lowercase).collect(),
                        _ => vec![],
                    };
                    merge_keys.insert(stem.clone(), keys);
                }
                if let Some(tests) = v.get("tests").and_then(|t| t.as_array()) {
                    for t in tests {
                        if t.get("kind").and_then(|k| k.as_str()) != Some("unique") {
                            continue;
                        }
                        let mut cols = BTreeSet::new();
                        if let Some(c) = t.get("columns").and_then(|c| c.as_array()) {
                            cols.extend(c.iter().filter_map(|x| x.as_str()).map(str::to_lowercase));
                        }
                        if let Some(c) = t.get("column").and_then(|c| c.as_str()) {
                            cols.insert(c.to_lowercase());
                        }
                        if !cols.is_empty() {
                            uniques.entry(stem.clone()).or_default().push(cols);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Grain maps per variant, keyed by model stem.
    let grain_a: HashMap<String, Grain> = sqls
        .iter()
        .map(|(m, s)| {
            let keys = merge_keys.get(m).cloned().unwrap_or_default();
            (m.clone(), grain::grain_of(s, &keys, DeclaredKeyKind::MergeRowKey))
        })
        .collect();
    let with_tests = |base: &HashMap<String, Grain>| -> HashMap<String, Grain> {
        let mut out = base.clone();
        for (m, sets) in &uniques {
            if matches!(out.get(m), Some(Grain::Known(_))) {
                continue;
            }
            // smallest declared unique set
            if let Some(s) = sets.iter().min_by_key(|s| s.len()) {
                out.insert(m.clone(), Grain::Known(s.clone()));
            }
        }
        out
    };
    let grain_b = with_tests(&grain_a);
    let sqls_c: HashMap<String, String> = sqls.iter().map(|(m, s)| (m.clone(), resolve_ordinals(s))).collect();
    let grain_c_base: HashMap<String, Grain> = sqls_c
        .iter()
        .map(|(m, s)| {
            let keys = merge_keys.get(m).cloned().unwrap_or_default();
            (m.clone(), grain::grain_of(s, &keys, DeclaredKeyKind::MergeRowKey))
        })
        .collect();
    let grain_c = with_tests(&grain_c_base);

    for t in &targets {
        let Some(sql) = sqls.get(t) else {
            println!("{t}\tMISSING\t-\t-\t-\t-");
            continue;
        };
        if let Ok(d) = std::env::var("GRAIN_DUMP") {
            let _ = std::fs::write(format!("{d}/{t}.sql"), sql);
        }
        let parse = match rocky_sql::parser::parse_single_statement(sql) {
            Ok(_) => "ok".to_string(),
            Err(e) => format!("FAIL:{}", e.to_string().replace(['\t', '\n'], " ").chars().take(120).collect::<String>()),
        };
        for (variant, gm, s) in [("A", &grain_a, sql), ("B", &grain_b, sql), ("C", &grain_c, &sqls_c[t])] {
            let mut upstream: HashMap<String, Grain> = HashMap::new();
            for site in grain::join_sites(s) {
                let stem = site.relation.rsplit('.').next().unwrap_or(&site.relation).to_string();
                if let Some(Grain::Known(g)) = gm.get(&stem) {
                    upstream.insert(site.relation.clone(), Grain::Known(g.clone()));
                }
            }
            let diags = grain::check_fanout(t, s, &upstream);
            let own = match gm.get(t) {
                Some(Grain::Known(g)) => g.iter().cloned().collect::<Vec<_>>().join("+"),
                _ => "unknown".into(),
            };
            let msgs: Vec<String> = diags.iter().map(|d| d.message.to_string()).collect();
            println!("{t}\t{parse}\t{variant}\t{}\t{own}\t{}", diags.len(), msgs.join(" || "));
        }
    }
}

/// Make importer output parseable: `@var(x)` -> resolved relation name;
/// strip leftover Jinja.
fn preprocess(raw: &str, var_map: &HashMap<String, String>) -> String {
    let re_var = Regex::new(r"@var\(\s*([A-Za-z0-9_]+)\s*(?:,[^)]*)?\)").unwrap();
    let s = re_var.replace_all(raw, |c: &regex::Captures| {
        let v = c[1].to_lowercase();
        var_map.get(&v).cloned().unwrap_or(v)
    });
    let re_block = Regex::new(r"(?s)\{%.*?%\}").unwrap();
    let s = re_block.replace_all(&s, "");
    let re_expr = Regex::new(r"(?s)\{\{.*?\}\}").unwrap();
    let s = re_expr.replace_all(&s, "NULL");
    let re_comment = Regex::new(r"(?s)\{#.*?#\}").unwrap();
    let s = re_comment.replace_all(&s, "").trim().trim_end_matches(';').to_string();
    // Importer placeholders for untranslated Jinja expressions: first try a
    // single NULL per run of placeholders; if that does not parse, drop them.
    let re_todo = Regex::new(r"(?:/\* TODO: unsupported Jinja expression \*/\s*)+").unwrap();
    let re_lead = Regex::new(r"^(?:\s*(?:--[^\n]*\n|/\*.*?\*/))*").unwrap();
    let s = re_lead.replace(&s, "").to_string();
    let with_null = re_todo.replace_all(&s, " NULL ").to_string();
    if rocky_sql::parser::parse_single_statement(&with_null).is_ok() {
        return with_null;
    }
    let dropped = re_todo.replace_all(&s, " ").to_string();
    if rocky_sql::parser::parse_single_statement(&dropped).is_ok() {
        return dropped;
    }
    with_null
}

/// Secondary only: rewrite `GROUP BY 1,2,...` to the projected column names
/// (bare column or alias) of the same SELECT. Textual and best-effort.
fn resolve_ordinals(sql: &str) -> String {
    use sqlparser::ast::{Expr, GroupByExpr, SelectItem, SetExpr, Statement, Value};
    let Ok(Statement::Query(mut q)) = rocky_sql::parser::parse_single_statement(sql) else {
        return sql.to_string();
    };
    fn fix_query(q: &mut sqlparser::ast::Query) {
        if let Some(w) = q.with.as_mut() {
            for c in &mut w.cte_tables {
                fix_query(&mut c.query);
            }
        }
        if let SetExpr::Select(sel) = q.body.as_mut() {
            let proj = sel.projection.clone();
            if let GroupByExpr::Expressions(exprs, _) = &mut sel.group_by {
                for e in exprs.iter_mut() {
                    let n = match e {
                        Expr::Value(v) => match &v.value {
                            Value::Number(n, _) => n.parse::<usize>().ok(),
                            _ => None,
                        },
                        _ => None,
                    };
                    if let Some(n) = n {
                        match proj.get(n.wrapping_sub(1)) {
                            Some(SelectItem::UnnamedExpr(x)) => *e = x.clone(),
                            Some(SelectItem::ExprWithAlias { alias, .. }) => {
                                *e = Expr::Identifier(alias.clone())
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
    fix_query(&mut q);
    q.to_string()
}
