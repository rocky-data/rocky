//! Static documentation site renderer.
//!
//! [`render_site`] turns a [`ProjectDocs`] into a set of files for one output
//! directory: an overview page, one page per model and per source, an
//! interactive lineage page, and shared assets. The site needs no server and
//! makes no network requests, so it opens from `file://` and can be hosted on
//! any static host.
//!
//! Model and source pages are plain server-rendered HTML, so they read
//! without JavaScript. Search and the lineage graph are progressive
//! enhancement on top of `assets/data.js`.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::json;

use crate::project_docs::{DocColumnEdge, DocTest, ModelDetail, ProjectDocs};

const SITE_CSS: &str = include_str!("docs_site/site.css");
const SITE_JS: &str = include_str!("docs_site/site.js");

/// One file of the rendered site, with a `/`-separated path relative to the
/// output directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteFile {
    /// Relative path, for example `models/orders.html`.
    pub path: String,
    /// File contents.
    pub content: String,
}

fn esc(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// File-name-safe slugs, unique per kind.
struct Slugs {
    models: HashMap<String, String>,
    sources: HashMap<String, String>,
}

impl Slugs {
    fn new(docs: &ProjectDocs) -> Self {
        Self {
            models: assign(docs.index.models.iter().map(|m| m.name.as_str())),
            sources: assign(docs.sources.iter().map(|s| s.name.as_str())),
        }
    }

    /// Link target for a model or source, relative to `root` (`""` or `"../"`).
    fn href(&self, root: &str, name: &str) -> Option<String> {
        if let Some(slug) = self.models.get(name) {
            return Some(format!("{root}models/{slug}.html"));
        }
        self.sources
            .get(name)
            .map(|slug| format!("{root}sources/{slug}.html"))
    }

    fn link(&self, root: &str, name: &str) -> String {
        match self.href(root, name) {
            Some(href) => format!("<a href=\"{}\">{}</a>", esc(&href), esc(name)),
            None => esc(name),
        }
    }
}

fn assign<'a>(names: impl Iterator<Item = &'a str>) -> HashMap<String, String> {
    let mut used: HashSet<String> = HashSet::new();
    let mut out = HashMap::new();
    for name in names {
        let base: String = name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let base = if base.is_empty() || base.chars().all(|c| c == '.') {
            "_".to_string()
        } else {
            base
        };
        let mut slug = base.clone();
        let mut n = 2;
        // Case-insensitive file systems treat `Orders` and `orders` as one file.
        while !used.insert(slug.to_ascii_lowercase()) {
            slug = format!("{base}-{n}");
            n += 1;
        }
        out.insert(name.to_string(), slug);
    }
    out
}

fn shell(title: &str, root: &str, active: &str, body: &str) -> String {
    let nav = |key: &str, label: &str, href: &str| {
        format!(
            "<a href=\"{root}{href}\"{}>{label}</a>",
            if key == active {
                " class=\"active\""
            } else {
                ""
            }
        )
    };
    format!(
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n\
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
<title>{title} - Rocky docs</title>\n<link rel=\"stylesheet\" href=\"{root}assets/site.css\">\n</head>\n\
<body data-root=\"{root}\">\n<header class=\"top\">\n<span class=\"brand\">Rocky</span>\n<nav>{overview}{lineage}</nav>\n\
<div class=\"search-wrap\"><input id=\"search\" type=\"search\" placeholder=\"Search models and columns (press /)\" autocomplete=\"off\">\
<div id=\"search-results\" hidden></div></div>\n</header>\n{body}\n\
<script src=\"{root}assets/data.js\"></script>\n<script src=\"{root}assets/site.js\"></script>\n</body>\n</html>\n",
        title = esc(title),
        overview = nav("overview", "Overview", "index.html"),
        lineage = nav("lineage", "Lineage", "lineage.html"),
    )
}

fn duration_label(seconds: u64) -> String {
    match seconds {
        s if s != 0 && s % 86_400 == 0 => format!("{} d", s / 86_400),
        s if s != 0 && s % 3_600 == 0 => format!("{} h", s / 3_600),
        s if s != 0 && s % 60 == 0 => format!("{} min", s / 60),
        s => format!("{s} s"),
    }
}

/// Render the whole site.
pub fn render_site(docs: &ProjectDocs) -> Vec<SiteFile> {
    let slugs = Slugs::new(docs);
    let mut files = vec![
        SiteFile {
            path: "index.html".into(),
            content: render_overview(docs, &slugs),
        },
        SiteFile {
            path: "lineage.html".into(),
            content: render_lineage_page(),
        },
        SiteFile {
            path: "assets/site.css".into(),
            content: SITE_CSS.into(),
        },
        SiteFile {
            path: "assets/site.js".into(),
            content: SITE_JS.into(),
        },
        SiteFile {
            path: "assets/data.js".into(),
            content: render_data_js(docs, &slugs),
        },
    ];
    for model in &docs.index.models {
        let slug = &slugs.models[&model.name];
        files.push(SiteFile {
            path: format!("models/{slug}.html"),
            content: render_model_page(docs, &slugs, &model.name),
        });
    }
    for source in &docs.sources {
        let slug = &slugs.sources[&source.name];
        files.push(SiteFile {
            path: format!("sources/{slug}.html"),
            content: render_source_page(docs, &slugs, &source.name),
        });
    }
    files
}

fn render_data_js(docs: &ProjectDocs, slugs: &Slugs) -> String {
    let models: Vec<_> = docs
        .index
        .models
        .iter()
        .map(|m| {
            json!({
                "name": m.name,
                "slug": slugs.models[&m.name],
                "description": m.description,
                "strategy": m.strategy,
                "columns": m.columns.iter().map(|c| json!({
                    "name": c.name,
                    "type": c.data_type,
                    "description": c.description,
                })).collect::<Vec<_>>(),
            })
        })
        .collect();
    let sources: Vec<_> = docs
        .sources
        .iter()
        .map(|s| {
            json!({
                "name": s.name,
                "slug": slugs.sources[&s.name],
                "columns": s.columns,
            })
        })
        .collect();
    let edges: Vec<_> = docs
        .model_edges()
        .into_iter()
        .map(|(a, b)| json!([a, b]))
        .collect();
    let column_lineage: Vec<_> = docs
        .column_lineage
        .iter()
        .map(|e| {
            json!([
                e.source_model,
                e.source_column,
                e.target_model,
                e.target_column,
                e.transform
            ])
        })
        .collect();
    let data = json!({
        "models": models,
        "sources": sources,
        "edges": edges,
        "column_lineage": column_lineage,
    });
    // `<` is escaped so the payload stays inert if a host ever inlines it.
    let body = data.to_string().replace('<', "\\u003c");
    format!("window.ROCKY_DOCS = {body};\n")
}

fn render_lineage_page() -> String {
    let body = "<div class=\"toolbar\"><button id=\"dag-reset\" type=\"button\">Reset view</button>\
<button id=\"dag-all\" type=\"button\" hidden>Show whole project</button>\
<span class=\"muted\">Scroll to zoom, drag to pan, click a node to trace it.</span></div>\n\
<div class=\"dag-page\"><svg id=\"dag\" role=\"img\" aria-label=\"Model lineage graph\"></svg>\
<aside id=\"panel\"></aside></div>";
    shell("Lineage", "", "lineage", body)
}

fn render_overview(docs: &ProjectDocs, slugs: &Slugs) -> String {
    let idx = &docs.index;
    let mut b = String::new();
    b.push_str("<main>\n<h1>Project documentation</h1>\n");
    b.push_str(&format!(
        "<div class=\"stats\"><div class=\"stat\"><b>{}</b>models</div><div class=\"stat\"><b>{}</b>sources</div>\
<div class=\"stat\"><b>{}</b>pipelines</div><div class=\"stat\"><b>{}</b>adapters</div>\
<div class=\"stat\"><b>{}</b>tests</div></div>\n",
        idx.models.len(),
        docs.sources.len(),
        idx.pipeline_count,
        idx.adapter_count,
        idx.models.iter().map(|m| m.tests.len()).sum::<usize>(),
    ));
    if !docs.schema_available {
        b.push_str(
            "<p class=\"muted\">Column types and lineage are missing because the project did not compile. \
Run <code>rocky compile</code> to see why.</p>\n",
        );
    }
    b.push_str("<h2>Models</h2>\n<p><input id=\"filter\" type=\"search\" placeholder=\"Filter models\" autocomplete=\"off\"></p>\n");
    b.push_str(
        "<table class=\"filterable\"><thead><tr><th>Model</th><th>Description</th><th>Strategy</th>\
<th>Columns</th><th>Tests</th><th>Contract</th><th>Freshness</th></tr></thead><tbody>\n",
    );
    for m in &idx.models {
        let detail = docs.details.get(&m.name);
        let hay = format!(
            "{} {}",
            m.name,
            m.description.as_deref().unwrap_or_default()
        )
        .to_lowercase();
        b.push_str(&format!(
            "<tr data-hay=\"{}\"><td>{}</td><td>{}</td><td><span class=\"badge\">{}</span></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>\n",
            esc(&hay),
            slugs.link("", &m.name),
            esc(m.description.as_deref().unwrap_or_default()),
            esc(&m.strategy),
            m.columns.len(),
            m.tests.len(),
            if detail.is_some_and(|d| d.contract.is_some()) { "yes" } else { "" },
            detail
                .and_then(|d| d.freshness.as_ref())
                .map(|f| duration_label(f.max_lag_seconds))
                .unwrap_or_default(),
        ));
    }
    b.push_str("</tbody></table>\n");
    if !docs.sources.is_empty() {
        b.push_str("<h2>Sources</h2>\n<table><thead><tr><th>Table</th><th>Columns read</th><th>Used by</th></tr></thead><tbody>\n");
        for s in &docs.sources {
            b.push_str(&format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td></tr>\n",
                slugs.link("", &s.name),
                s.columns.len(),
                s.used_by
                    .iter()
                    .map(|n| slugs.link("", n))
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
        b.push_str("</tbody></table>\n");
    }
    b.push_str("</main>\n<footer>Generated by Rocky</footer>");
    shell("Overview", "", "overview", &b)
}

/// Column edges grouped by `(target_model, target_column)` and by
/// `(source_model, source_column)`.
struct ColumnIndex<'a> {
    into: BTreeMap<(&'a str, &'a str), Vec<&'a DocColumnEdge>>,
    from: BTreeMap<(&'a str, &'a str), Vec<&'a DocColumnEdge>>,
}

impl<'a> ColumnIndex<'a> {
    fn new(edges: &'a [DocColumnEdge]) -> Self {
        let mut into: BTreeMap<_, Vec<_>> = BTreeMap::new();
        let mut from: BTreeMap<_, Vec<_>> = BTreeMap::new();
        for e in edges {
            into.entry((e.target_model.as_str(), e.target_column.as_str()))
                .or_default()
                .push(e);
            from.entry((e.source_model.as_str(), e.source_column.as_str()))
                .or_default()
                .push(e);
        }
        Self { into, from }
    }
}

fn render_model_page(docs: &ProjectDocs, slugs: &Slugs, name: &str) -> String {
    let root = "../";
    let Some(model) = docs.index.models.iter().find(|m| m.name == name) else {
        return shell(name, root, "overview", "<main></main>");
    };
    let empty = ModelDetail::default();
    let detail = docs.details.get(name).unwrap_or(&empty);
    let col_index = ColumnIndex::new(&docs.column_lineage);
    let mut b = String::new();
    b.push_str(&format!(
        "<main>\n<div class=\"crumbs\"><a href=\"{root}index.html\">Models</a> / {}</div>\n<h1>{}</h1>\n",
        esc(name),
        esc(name)
    ));
    b.push_str(&format!(
        "<p><span class=\"badge\">{}</span>{}<a href=\"{root}lineage.html#focus={}\">View in lineage graph</a></p>\n",
        esc(&model.strategy),
        model
            .governance
            .as_ref()
            .map(|g| format!("<span class=\"badge\">{}</span>", esc(&g.access)))
            .unwrap_or_default(),
        esc(&url_encode(name)),
    ));
    if let Some(desc) = &model.description {
        b.push_str(&format!("<p>{}</p>\n", esc(desc)));
    }

    b.push_str("<h2>Details</h2>\n<table><tbody>\n");
    let mut row = |k: &str, v: String| {
        b.push_str(&format!("<tr><th>{k}</th><td>{v}</td></tr>\n"));
    };
    row("Target", format!("<code>{}</code>", esc(&model.target)));
    if !detail.file.is_empty() {
        row("File", format!("<code>{}</code>", esc(&detail.file)));
    }
    if let Some(g) = &model.governance {
        if let Some(v) = &g.group {
            row("Group", esc(v));
        }
        if let Some(v) = &g.owner {
            row("Owner", esc(v));
        }
        if let Some(v) = &g.version {
            row("Version", esc(v));
        }
        if let Some(v) = &g.deprecation_date {
            row("Deprecated after", esc(v));
        }
    }
    if let Some(f) = &detail.freshness {
        let mut text = format!("within {}", duration_label(f.max_lag_seconds));
        if let Some(c) = &f.time_column {
            text.push_str(&format!(" on <code>{}</code>", esc(c)));
        }
        if let Some(s) = &f.severity {
            text.push_str(&format!(" ({})", esc(s)));
        }
        row("Freshness", text);
    }
    if !detail.tags.is_empty() {
        row(
            "Tags",
            detail
                .tags
                .iter()
                .map(|(k, v)| {
                    if v.is_empty() {
                        format!("<span class=\"badge\">{}</span>", esc(k))
                    } else {
                        format!("<span class=\"badge\">{}={}</span>", esc(k), esc(v))
                    }
                })
                .collect::<String>(),
        );
    }
    b.push_str("</tbody></table>\n");

    // Columns.
    b.push_str("<h2>Columns</h2>\n");
    if model.columns.is_empty() {
        b.push_str("<p class=\"muted\">No column metadata available.</p>\n");
    } else {
        b.push_str("<table><thead><tr><th>Column</th><th>Type</th><th>Nullable</th><th>Classification</th><th>Description and lineage</th></tr></thead><tbody>\n");
        for c in &model.columns {
            let class = detail
                .classification
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(&c.name))
                .map(|(_, v)| format!("<span class=\"badge\">{}</span>", esc(v)))
                .unwrap_or_default();
            let mut lin = String::new();
            let key = (model.name.as_str(), c.name.as_str());
            if let Some(edges) = col_index.into.get(&key) {
                lin.push_str("<span class=\"lin\">from ");
                lin.push_str(&column_links(slugs, root, edges, true));
                lin.push_str("</span>");
            }
            if let Some(edges) = col_index.from.get(&key) {
                lin.push_str("<span class=\"lin\">feeds ");
                lin.push_str(&column_links(slugs, root, edges, false));
                lin.push_str("</span>");
            }
            b.push_str(&format!(
                "<tr id=\"col-{id}\"><td><code>{name}</code></td><td><code>{ty}</code></td><td>{null}</td><td>{class}</td><td>{desc}{lin}</td></tr>\n",
                id = esc(&c.name),
                name = esc(&c.name),
                ty = esc(&c.data_type),
                null = if c.nullable { "yes" } else { "no" },
                desc = esc(c.description.as_deref().unwrap_or_default()),
            ));
        }
        b.push_str("</tbody></table>\n");
    }

    // Tests.
    b.push_str("<h2>Tests</h2>\n");
    if model.tests.is_empty() {
        b.push_str("<p class=\"muted\">No tests declared.</p>\n");
    } else {
        b.push_str("<table><thead><tr><th>Test</th><th>Column</th><th>Severity</th><th>Parameters</th></tr></thead><tbody>\n");
        for t in model.tests.iter().map(DocTest::from_decl) {
            b.push_str(&format!(
                "<tr><td><code>{}</code></td><td>{}</td><td><span class=\"badge {sev}\">{sev}</span></td><td><code>{}</code>{}</td></tr>\n",
                esc(&t.kind),
                t.column.as_deref().map(|c| format!("<code>{}</code>", esc(c))).unwrap_or_default(),
                if t.params == "{}" { String::new() } else { esc(&t.params) },
                t.filter
                    .as_deref()
                    .map(|f| format!(" where <code>{}</code>", esc(f)))
                    .unwrap_or_default(),
                sev = esc(&t.severity),
            ));
        }
        b.push_str("</tbody></table>\n");
    }

    // Contract.
    b.push_str("<h2>Contract</h2>\n");
    match &detail.contract {
        None => b.push_str("<p class=\"muted\">No contract.</p>\n"),
        Some(contract) => {
            let flat = |v: &[String]| {
                v.iter()
                    .map(|c| format!("<code>{}</code>", esc(c)))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            b.push_str("<table><tbody>\n");
            if !contract.required.is_empty() {
                b.push_str(&format!(
                    "<tr><th>Required columns</th><td>{}</td></tr>\n",
                    flat(&contract.required)
                ));
            }
            if !contract.protected.is_empty() {
                b.push_str(&format!(
                    "<tr><th>Protected columns</th><td>{}</td></tr>\n",
                    flat(&contract.protected)
                ));
            }
            if contract.no_new_nullable {
                b.push_str("<tr><th>New nullable columns</th><td>refused</td></tr>\n");
            }
            b.push_str("</tbody></table>\n");
            if !contract.columns.is_empty() {
                b.push_str("<table><thead><tr><th>Column</th><th>Type</th><th>Nullable</th><th>Description</th></tr></thead><tbody>\n");
                for c in &contract.columns {
                    b.push_str(&format!(
                        "<tr><td><code>{}</code></td><td><code>{}</code></td><td>{}</td><td>{}</td></tr>\n",
                        esc(&c.name),
                        esc(c.type_name.as_deref().unwrap_or_default()),
                        match c.nullable {
                            Some(true) => "yes",
                            Some(false) => "no",
                            None => "",
                        },
                        esc(c.description.as_deref().unwrap_or_default()),
                    ));
                }
                b.push_str("</tbody></table>\n");
            }
        }
    }

    // Upstream and downstream.
    let downstream: Vec<&str> = docs
        .index
        .models
        .iter()
        .filter(|m| m.depends_on.iter().any(|d| d == name))
        .map(|m| m.name.as_str())
        .collect();
    let mut upstream: Vec<String> = model.depends_on.clone();
    for s in &detail.declared_sources {
        if !upstream.contains(s) {
            upstream.push(s.clone());
        }
    }
    for e in docs
        .column_lineage
        .iter()
        .filter(|e| e.target_model == name)
    {
        if !upstream.contains(&e.source_model) {
            upstream.push(e.source_model.clone());
        }
    }
    upstream.sort();
    b.push_str("<h2>Lineage</h2>\n<div class=\"row\"><div><h3>Upstream</h3>");
    b.push_str(&link_list(slugs, root, upstream.iter().map(String::as_str)));
    b.push_str("</div><div><h3>Downstream</h3>");
    b.push_str(&link_list(slugs, root, downstream.iter().copied()));
    b.push_str("</div></div>\n");

    if !detail.sql.is_empty() {
        b.push_str(&format!(
            "<h2>SQL</h2>\n<pre><code>{}</code></pre>\n",
            esc(&detail.sql)
        ));
    }
    b.push_str("</main>\n<footer>Generated by Rocky</footer>");
    shell(name, root, "overview", &b)
}

fn render_source_page(docs: &ProjectDocs, slugs: &Slugs, name: &str) -> String {
    let root = "../";
    let Some(source) = docs.sources.iter().find(|s| s.name == name) else {
        return shell(name, root, "overview", "<main></main>");
    };
    let mut b = String::new();
    b.push_str(&format!(
        "<main>\n<div class=\"crumbs\"><a href=\"{root}index.html\">Sources</a> / {}</div>\n<h1>{}</h1>\n<p class=\"muted\">External table read by the project's models.</p>\n",
        esc(name),
        esc(name)
    ));
    b.push_str(&format!(
        "<p><a href=\"{root}lineage.html#focus={}\">View in lineage graph</a></p>\n<h2>Columns read</h2>\n",
        esc(&url_encode(name))
    ));
    if source.columns.is_empty() {
        b.push_str("<p class=\"muted\">No column-level reads recorded.</p>\n");
    } else {
        b.push_str("<table><thead><tr><th>Column</th><th>Feeds</th></tr></thead><tbody>\n");
        let col_index = ColumnIndex::new(&docs.column_lineage);
        for c in &source.columns {
            let feeds = col_index
                .from
                .get(&(name, c.as_str()))
                .map(|edges| column_links(slugs, root, edges, false))
                .unwrap_or_default();
            b.push_str(&format!(
                "<tr><td><code>{}</code></td><td>{feeds}</td></tr>\n",
                esc(c)
            ));
        }
        b.push_str("</tbody></table>\n");
    }
    b.push_str("<h2>Used by</h2>\n");
    b.push_str(&link_list(
        slugs,
        root,
        source.used_by.iter().map(String::as_str),
    ));
    b.push_str("\n</main>\n<footer>Generated by Rocky</footer>");
    shell(name, root, "overview", &b)
}

fn link_list<'a>(slugs: &Slugs, root: &str, names: impl Iterator<Item = &'a str>) -> String {
    let items: Vec<String> = names
        .map(|n| format!("<li>{}</li>", slugs.link(root, n)))
        .collect();
    if items.is_empty() {
        "<p class=\"muted\">None.</p>".to_string()
    } else {
        format!("<ul class=\"links\">{}</ul>", items.join(""))
    }
}

/// Links to the far end of each edge: `model.column` with a link to the model.
fn column_links(slugs: &Slugs, root: &str, edges: &[&DocColumnEdge], upstream: bool) -> String {
    edges
        .iter()
        .map(|e| {
            let (model, column) = if upstream {
                (&e.source_model, &e.source_column)
            } else {
                (&e.target_model, &e.target_column)
            };
            let anchor = slugs
                .href(root, model)
                .map(|h| {
                    format!(
                        "<a href=\"{}#col-{}\">{}.{}</a>",
                        esc(&h),
                        esc(column),
                        esc(model),
                        esc(column)
                    )
                })
                .unwrap_or_else(|| format!("{}.{}", esc(model), esc(column)));
            if e.transform == "direct" {
                anchor
            } else {
                format!(
                    "{anchor} <span class=\"muted\">({})</span>",
                    esc(&e.transform)
                )
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Percent-encode everything outside the URL-unreserved set.
fn url_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::docs::{DocColumn, DocIndex, DocModel};
    use crate::project_docs::{
        DocContract, DocContractColumn, DocFreshness, DocSource, ModelDetail,
    };
    use crate::tests::{TestDecl, TestSeverity, TestType};
    use std::collections::BTreeMap;

    fn column(name: &str, ty: &str, nullable: bool, description: Option<&str>) -> DocColumn {
        DocColumn {
            name: name.into(),
            data_type: ty.into(),
            nullable,
            description: description.map(Into::into),
        }
    }

    fn sample() -> ProjectDocs {
        let orders = DocModel {
            name: "orders".into(),
            description: Some("One row per <order>".into()),
            target: "c.s.orders".into(),
            strategy: "full_refresh".into(),
            depends_on: vec![],
            columns: vec![
                column("id", "BIGINT", false, Some("Primary key")),
                column("email", "VARCHAR", true, None),
            ],
            tests: vec![TestDecl {
                test_type: TestType::NotNull,
                column: Some("id".into()),
                severity: TestSeverity::Error,
                filter: None,
            }],
            governance: None,
        };
        let report = DocModel {
            name: "report".into(),
            description: None,
            target: "c.s.report".into(),
            strategy: "view".into(),
            depends_on: vec!["orders".into()],
            columns: vec![column("order_id", "BIGINT", false, None)],
            tests: vec![],
            governance: None,
        };
        let mut details = BTreeMap::new();
        details.insert(
            "orders".to_string(),
            ModelDetail {
                file: "marts/orders.sql".into(),
                sql: "SELECT id FROM raw.orders WHERE a < b".into(),
                classification: [("email".to_string(), "pii".to_string())].into(),
                freshness: Some(DocFreshness {
                    max_lag_seconds: 7200,
                    time_column: Some("updated_at".into()),
                    severity: None,
                }),
                contract: Some(DocContract {
                    columns: vec![DocContractColumn {
                        name: "id".into(),
                        type_name: Some("Int64".into()),
                        nullable: Some(false),
                        description: None,
                    }],
                    required: vec!["id".into()],
                    protected: vec![],
                    no_new_nullable: true,
                }),
                ..ModelDetail::default()
            },
        );
        ProjectDocs {
            index: DocIndex {
                models: vec![orders, report],
                pipeline_count: 1,
                adapter_count: 1,
            },
            details,
            sources: vec![DocSource {
                name: "raw.orders".into(),
                columns: vec!["id".into()],
                used_by: vec!["orders".into()],
            }],
            column_lineage: vec![
                DocColumnEdge {
                    source_model: "raw.orders".into(),
                    source_column: "id".into(),
                    target_model: "orders".into(),
                    target_column: "id".into(),
                    transform: "direct".into(),
                },
                DocColumnEdge {
                    source_model: "orders".into(),
                    source_column: "id".into(),
                    target_model: "report".into(),
                    target_column: "order_id".into(),
                    transform: "cast".into(),
                },
            ],
            schema_available: true,
        }
    }

    fn file<'a>(files: &'a [SiteFile], path: &str) -> &'a str {
        &files
            .iter()
            .find(|f| f.path == path)
            .unwrap_or_else(|| panic!("missing {path}"))
            .content
    }

    #[test]
    fn site_has_expected_files() {
        let files = render_site(&sample());
        let mut paths: Vec<_> = files.iter().map(|f| f.path.as_str()).collect();
        paths.sort_unstable();
        assert_eq!(
            paths,
            vec![
                "assets/data.js",
                "assets/site.css",
                "assets/site.js",
                "index.html",
                "lineage.html",
                "models/orders.html",
                "models/report.html",
                "sources/raw.orders.html",
            ]
        );
    }

    #[test]
    fn model_page_shows_columns_tests_contract_freshness_and_lineage() {
        let files = render_site(&sample());
        let page = file(&files, "models/orders.html");
        assert!(page.contains("id=\"col-id\""));
        assert!(page.contains("Primary key"));
        assert!(page.contains("pii"), "classification badge");
        assert!(page.contains("not_null"));
        assert!(page.contains("within 2 h"));
        assert!(page.contains("<code>updated_at</code>"));
        assert!(page.contains("Required columns"));
        assert!(page.contains("refused"), "no_new_nullable");
        assert!(page.contains("marts/orders.sql"));
        assert!(page.contains("../sources/raw.orders.html#col-id"));
        assert!(page.contains("../models/report.html#col-order_id"));
        assert!(page.contains("lineage.html#focus=orders"));
        assert!(page.contains("assets/data.js"));
    }

    #[test]
    fn untrusted_text_is_escaped() {
        let files = render_site(&sample());
        let overview = file(&files, "index.html");
        assert!(overview.contains("One row per &lt;order&gt;"));
        assert!(!overview.contains("<order>"));
        let page = file(&files, "models/orders.html");
        assert!(page.contains("WHERE a &lt; b"));
    }

    #[test]
    fn source_and_downstream_links_resolve() {
        let files = render_site(&sample());
        let source = file(&files, "sources/raw.orders.html");
        assert!(source.contains("../models/orders.html"));
        let report = file(&files, "models/report.html");
        assert!(report.contains("../models/orders.html"));
        let orders = file(&files, "models/orders.html");
        assert!(orders.contains(
            "<h3>Downstream</h3><ul class=\"links\"><li><a href=\"../models/report.html\">"
        ));
    }

    #[test]
    fn data_js_carries_search_and_lineage_data() {
        let files = render_site(&sample());
        let data = file(&files, "assets/data.js");
        assert!(data.starts_with("window.ROCKY_DOCS = {"));
        let json: serde_json::Value = serde_json::from_str(
            data.trim_start_matches("window.ROCKY_DOCS = ")
                .trim_end()
                .trim_end_matches(';'),
        )
        .expect("data.js payload is JSON");
        assert_eq!(json["models"].as_array().unwrap().len(), 2);
        assert_eq!(json["sources"][0]["name"], "raw.orders");
        assert!(
            json["edges"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e[0] == "orders" && e[1] == "report")
        );
        assert_eq!(json["column_lineage"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn site_makes_no_external_requests() {
        for f in render_site(&sample()) {
            // The SVG namespace URI is an identifier, not a request.
            let content = f.content.replace("http://www.w3.org/2000/svg", "");
            for needle in ["http://", "https://", "//cdn", "@import", "src=\"//"] {
                assert!(!content.contains(needle), "{} contains {needle}", f.path);
            }
        }
    }

    #[test]
    fn colliding_slugs_stay_unique() {
        let slugs = assign(["a/b", "a_b", "A_B"].into_iter());
        let mut values: Vec<_> = slugs.values().map(|s| s.to_ascii_lowercase()).collect();
        values.sort_unstable();
        values.dedup();
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn empty_project_renders() {
        let docs = ProjectDocs {
            index: DocIndex {
                models: vec![],
                pipeline_count: 0,
                adapter_count: 0,
            },
            details: BTreeMap::new(),
            sources: vec![],
            column_lineage: vec![],
            schema_available: false,
        };
        let files = render_site(&docs);
        assert!(file(&files, "index.html").contains("did not compile"));
    }
}
