//! Minimal web dashboard for `rocky serve`.
//!
//! Renders an embedded HTML page showing project status at a glance:
//! project name, pipelines, adapters, last run status, and links to
//! API endpoints. No external dependencies, no build step.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Html;

use crate::state::ServerState;

/// Serve the dashboard HTML page.
pub async fn dashboard(State(state): State<Arc<ServerState>>) -> Result<Html<String>, StatusCode> {
    let project_info = gather_project_info(&state).await;
    Ok(Html(render_html(&project_info)))
}

/// All the data we display on the dashboard.
struct ProjectInfo {
    project_name: String,
    config_path: String,
    pipelines: Vec<PipelineInfo>,
    adapters: Vec<AdapterInfo>,
    models: Option<ModelsInfo>,
    last_run: Option<RunInfo>,
    diagnostics_count: usize,
    has_errors: bool,
}

struct PipelineInfo {
    name: String,
    pipeline_type: String,
}

struct AdapterInfo {
    name: String,
    adapter_type: String,
}

struct ModelsInfo {
    count: usize,
    names: Vec<String>,
}

struct RunInfo {
    run_id: String,
    started_at: String,
    finished_at: String,
    status: String,
    models_executed: usize,
    trigger: String,
}

/// Gather project information from config, compile state, and run history.
async fn gather_project_info(state: &ServerState) -> ProjectInfo {
    // Read pipelines and adapters from config (if path provided).
    let (project_name, pipelines, adapters, config_path) =
        if let Some(ref cfg_path) = state.config_path {
            let display_path = cfg_path.display().to_string();
            match rocky_core::config::load_rocky_config(cfg_path) {
                Ok(config) => {
                    let name = cfg_path
                        .parent()
                        .and_then(|p| p.file_name())
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| "rocky".into());

                    let pipelines: Vec<PipelineInfo> = config
                        .pipelines
                        .iter()
                        .map(|(name, cfg)| PipelineInfo {
                            name: name.clone(),
                            pipeline_type: pipeline_type_label(cfg),
                        })
                        .collect();

                    let adapters: Vec<AdapterInfo> = config
                        .adapters
                        .iter()
                        .map(|(name, cfg)| AdapterInfo {
                            name: name.clone(),
                            adapter_type: cfg.adapter_type.clone(),
                        })
                        .collect();

                    (name, pipelines, adapters, display_path)
                }
                Err(_) => ("rocky".into(), Vec::new(), Vec::new(), display_path),
            }
        } else {
            ("rocky".into(), Vec::new(), Vec::new(), "(none)".into())
        };

    // Read compilation status.
    let (models, diagnostics_count, has_errors) = {
        let lock = state.compile_result.read().await;
        match lock.as_ref() {
            Some(result) => {
                let names: Vec<String> = result.semantic_graph.models.keys().cloned().collect();
                let count = names.len();
                (
                    Some(ModelsInfo { count, names }),
                    result.diagnostics.len(),
                    result.has_errors,
                )
            }
            None => (None, 0, false),
        }
    };

    // Read last run from state store.
    let last_run = {
        let state_path = rocky_core::state::resolve_state_path(None, &state.models_dir).path;
        if state_path.exists() {
            rocky_core::state::StateStore::open_read_only(&state_path)
                .ok()
                .and_then(|store| store.list_runs(1).ok())
                .and_then(|runs| runs.into_iter().next())
                .map(|r| RunInfo {
                    run_id: r.run_id,
                    started_at: r.started_at.to_rfc3339(),
                    finished_at: r.finished_at.to_rfc3339(),
                    status: format!("{:?}", r.status),
                    models_executed: r.models_executed.len(),
                    trigger: format!("{:?}", r.trigger),
                })
        } else {
            None
        }
    };

    ProjectInfo {
        project_name,
        config_path,
        pipelines,
        adapters,
        models,
        last_run,
        diagnostics_count,
        has_errors,
    }
}

/// Label for pipeline variant without exposing internal enum details.
fn pipeline_type_label(cfg: &rocky_core::config::PipelineConfig) -> String {
    match cfg {
        rocky_core::config::PipelineConfig::Replication(_) => "replication".into(),
        rocky_core::config::PipelineConfig::Transformation(_) => "transformation".into(),
        rocky_core::config::PipelineConfig::Quality(_) => "quality".into(),
        rocky_core::config::PipelineConfig::Snapshot(_) => "snapshot".into(),
        rocky_core::config::PipelineConfig::Load(_) => "load".into(),
    }
}

/// Escape HTML special characters.
fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Render the full dashboard HTML.
fn render_html(info: &ProjectInfo) -> String {
    let mut html = String::with_capacity(8192);

    // --- Head ---
    html.push_str(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rocky Dashboard</title>
<style>
  :root {
    --bg: #0d1117;
    --surface: #161b22;
    --border: #30363d;
    --text: #c9d1d9;
    --text-dim: #8b949e;
    --accent: #58a6ff;
    --green: #3fb950;
    --red: #f85149;
    --orange: #d29922;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', monospace;
    background: var(--bg);
    color: var(--text);
    line-height: 1.6;
    padding: 2rem;
    max-width: 960px;
    margin: 0 auto;
  }
  h1 { font-size: 1.5rem; margin-bottom: 0.25rem; }
  h2 {
    font-size: 0.875rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    color: var(--text-dim);
    margin: 1.5rem 0 0.75rem;
    border-bottom: 1px solid var(--border);
    padding-bottom: 0.25rem;
  }
  .header {
    display: flex;
    align-items: baseline;
    gap: 1rem;
    margin-bottom: 0.5rem;
  }
  .header .version {
    font-size: 0.75rem;
    color: var(--text-dim);
  }
  .config-path {
    font-size: 0.75rem;
    color: var(--text-dim);
    margin-bottom: 1rem;
  }
  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 1rem;
    margin-bottom: 0.75rem;
  }
  .grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 0.75rem;
  }
  .stat {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0.5rem 0;
    border-bottom: 1px solid var(--border);
  }
  .stat:last-child { border-bottom: none; }
  .stat-label { color: var(--text-dim); font-size: 0.8125rem; }
  .stat-value { font-weight: 600; }
  .badge {
    display: inline-block;
    padding: 0.125rem 0.5rem;
    border-radius: 3px;
    font-size: 0.75rem;
    font-weight: 600;
  }
  .badge-green { background: rgba(63,185,80,0.15); color: var(--green); }
  .badge-red { background: rgba(248,81,73,0.15); color: var(--red); }
  .badge-orange { background: rgba(210,153,34,0.15); color: var(--orange); }
  .badge-blue { background: rgba(88,166,255,0.15); color: var(--accent); }
  .tag {
    display: inline-block;
    padding: 0.125rem 0.375rem;
    border-radius: 3px;
    font-size: 0.6875rem;
    background: rgba(88,166,255,0.1);
    color: var(--accent);
    margin-right: 0.25rem;
  }
  .model-list {
    display: flex;
    flex-wrap: wrap;
    gap: 0.375rem;
    margin-top: 0.5rem;
  }
  .model-chip {
    font-size: 0.75rem;
    padding: 0.25rem 0.5rem;
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 3px;
    color: var(--text);
  }
  a { color: var(--accent); text-decoration: none; }
  a:hover { text-decoration: underline; }
  .endpoint-list {
    list-style: none;
    padding: 0;
  }
  .endpoint-list li {
    padding: 0.375rem 0;
    border-bottom: 1px solid var(--border);
    font-size: 0.8125rem;
  }
  .endpoint-list li:last-child { border-bottom: none; }
  .method {
    display: inline-block;
    width: 3rem;
    font-weight: 600;
    font-size: 0.6875rem;
    color: var(--green);
  }
  .method-post { color: var(--orange); }
  .no-data {
    color: var(--text-dim);
    font-style: italic;
    font-size: 0.8125rem;
  }
</style>
</head>
<body>
"#,
    );

    // --- Header ---
    html.push_str(&format!(
        r#"<div class="header">
  <h1>{}</h1>
  <span class="version">rocky serve</span>
</div>
<div class="config-path">{}</div>
"#,
        esc(&info.project_name),
        esc(&info.config_path),
    ));

    // --- Compilation status ---
    html.push_str(r#"<h2>Compilation</h2><div class="card">"#);
    if let Some(ref models) = info.models {
        let status_badge = if info.has_errors {
            r#"<span class="badge badge-red">errors</span>"#
        } else if info.diagnostics_count > 0 {
            r#"<span class="badge badge-orange">warnings</span>"#
        } else {
            r#"<span class="badge badge-green">ok</span>"#
        };
        html.push_str(&format!(
            r#"<div class="stat"><span class="stat-label">Status</span><span class="stat-value">{status_badge}</span></div>"#
        ));
        html.push_str(&format!(
            r#"<div class="stat"><span class="stat-label">Models</span><span class="stat-value">{}</span></div>"#,
            models.count
        ));
        html.push_str(&format!(
            r#"<div class="stat"><span class="stat-label">Diagnostics</span><span class="stat-value">{}</span></div>"#,
            info.diagnostics_count
        ));
    } else {
        html.push_str(r#"<p class="no-data">Compilation pending...</p>"#);
    }
    html.push_str("</div>");

    // --- Pipelines ---
    html.push_str(r#"<h2>Pipelines</h2>"#);
    if info.pipelines.is_empty() {
        html.push_str(r#"<div class="card"><p class="no-data">No pipelines configured</p></div>"#);
    } else {
        html.push_str(r#"<div class="grid">"#);
        for p in &info.pipelines {
            html.push_str(&format!(
                r#"<div class="card"><div class="stat"><span class="stat-value">{}</span><span class="tag">{}</span></div></div>"#,
                esc(&p.name),
                esc(&p.pipeline_type),
            ));
        }
        html.push_str("</div>");
    }

    // --- Adapters ---
    html.push_str(r#"<h2>Adapters</h2>"#);
    if info.adapters.is_empty() {
        html.push_str(r#"<div class="card"><p class="no-data">No adapters configured</p></div>"#);
    } else {
        html.push_str(r#"<div class="grid">"#);
        for a in &info.adapters {
            html.push_str(&format!(
                r#"<div class="card"><div class="stat"><span class="stat-value">{}</span><span class="tag">{}</span></div></div>"#,
                esc(&a.name),
                esc(&a.adapter_type),
            ));
        }
        html.push_str("</div>");
    }

    // --- Models ---
    if let Some(ref models) = info.models {
        html.push_str(r#"<h2>Models</h2><div class="card">"#);
        html.push_str(r#"<div class="model-list">"#);
        for name in &models.names {
            html.push_str(&format!(
                r#"<a class="model-chip" href="/api/v1/models/{}">{}</a>"#,
                esc(name),
                esc(name),
            ));
        }
        html.push_str("</div></div>");
    }

    // --- Last Run ---
    html.push_str(r#"<h2>Last Run</h2><div class="card">"#);
    if let Some(ref run) = info.last_run {
        let status_badge = match run.status.as_str() {
            "Success" => r#"<span class="badge badge-green">success</span>"#,
            "Failure" => r#"<span class="badge badge-red">failure</span>"#,
            "PartialFailure" => r#"<span class="badge badge-orange">partial failure</span>"#,
            _ => r#"<span class="badge badge-blue">unknown</span>"#,
        };
        html.push_str(&format!(
            r#"<div class="stat"><span class="stat-label">Status</span><span class="stat-value">{status_badge}</span></div>"#
        ));
        html.push_str(&format!(
            r#"<div class="stat"><span class="stat-label">Run ID</span><span class="stat-value" style="font-size:0.75rem">{}</span></div>"#,
            esc(&run.run_id),
        ));
        html.push_str(&format!(
            r#"<div class="stat"><span class="stat-label">Started</span><span class="stat-value" style="font-size:0.75rem">{}</span></div>"#,
            esc(&run.started_at),
        ));
        html.push_str(&format!(
            r#"<div class="stat"><span class="stat-label">Finished</span><span class="stat-value" style="font-size:0.75rem">{}</span></div>"#,
            esc(&run.finished_at),
        ));
        html.push_str(&format!(
            r#"<div class="stat"><span class="stat-label">Models executed</span><span class="stat-value">{}</span></div>"#,
            run.models_executed,
        ));
        html.push_str(&format!(
            r#"<div class="stat"><span class="stat-label">Trigger</span><span class="stat-value">{}</span></div>"#,
            esc(&run.trigger),
        ));
    } else {
        html.push_str(r#"<p class="no-data">No runs recorded</p>"#);
    }
    html.push_str("</div>");

    // --- API Endpoints ---
    html.push_str(r#"<h2>API Endpoints</h2><div class="card"><ul class="endpoint-list">"#);
    let endpoints = [
        ("GET", "/api/v1/health", "Health check"),
        ("GET", "/api/v1/models", "List all models"),
        ("GET", "/api/v1/models/{name}", "Model details"),
        ("GET", "/api/v1/models/{name}/lineage", "Model lineage"),
        ("GET", "/api/v1/models/{name}/lineage/{col}", "Trace column"),
        ("GET", "/api/v1/models/{name}/history", "Model history"),
        ("GET", "/api/v1/models/{name}/metrics", "Model metrics"),
        ("GET", "/api/v1/runs", "Run history"),
        ("GET", "/api/v1/compile", "Compile status"),
        ("POST", "/api/v1/compile", "Trigger recompile"),
        ("GET", "/api/v1/dag", "Full DAG"),
        ("GET", "/api/v1/dag/layers", "Execution layers"),
    ];
    for (method, path, desc) in endpoints {
        let method_class = if method == "POST" {
            "method method-post"
        } else {
            "method"
        };
        html.push_str(&format!(
            r#"<li><span class="{method_class}">{method}</span> <a href="{path}">{path}</a> <span class="stat-label">— {desc}</span></li>"#,
        ));
    }
    html.push_str("</ul></div>");

    // --- Footer ---
    html.push_str(
        r#"<div style="margin-top:2rem;text-align:center;font-size:0.6875rem;color:var(--text-dim)">rocky &mdash; rust sql transformation engine</div>
</body>
</html>"#,
    );

    html
}
