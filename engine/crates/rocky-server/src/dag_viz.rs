//! DAG visualization — serves an interactive HTML/JS DAG explorer.
//!
//! Renders the model dependency graph as an interactive web page
//! served by `rocky serve`. Uses D3.js for force-directed layout
//! with no build step — the HTML is embedded as a const string.

/// Returns the embedded HTML for the DAG visualization page.
///
/// The HTML includes:
/// - D3.js loaded from CDN for force-directed graph layout
/// - Interactive features: zoom, pan, node click (shows model details)
/// - Color coding by layer (source/staging/intermediate/marts)
/// - Search/filter by model name
///
/// The page fetches DAG data from `/api/dag` (JSON endpoint)
/// which the axum server provides from the compiled model graph.
pub fn dag_viz_html() -> &'static str {
    DAG_VIZ_HTML
}

const DAG_VIZ_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rocky — DAG Explorer</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0d1117; color: #c9d1d9; overflow: hidden; }
  #toolbar { position: fixed; top: 0; left: 0; right: 0; z-index: 10; background: #161b22; border-bottom: 1px solid #30363d; padding: 8px 16px; display: flex; align-items: center; gap: 12px; }
  #toolbar h1 { font-size: 14px; font-weight: 600; color: #58a6ff; }
  #search { background: #0d1117; border: 1px solid #30363d; color: #c9d1d9; padding: 4px 8px; border-radius: 4px; font-size: 13px; width: 240px; }
  #search:focus { outline: none; border-color: #58a6ff; }
  #stats { font-size: 12px; color: #8b949e; margin-left: auto; }
  #graph { width: 100vw; height: 100vh; padding-top: 44px; }
  svg { width: 100%; height: 100%; }
  .node circle { cursor: pointer; stroke-width: 2; }
  .node text { font-size: 10px; fill: #8b949e; pointer-events: none; }
  .node.highlighted circle { stroke: #f0883e !important; stroke-width: 3; }
  .node.highlighted text { fill: #f0883e; font-weight: bold; }
  .link { stroke: #30363d; stroke-opacity: 0.6; }
  .link.highlighted { stroke: #58a6ff; stroke-opacity: 1; stroke-width: 2; }
  #detail { position: fixed; top: 52px; right: 8px; width: 280px; background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 12px; display: none; z-index: 10; font-size: 12px; }
  #detail h3 { color: #58a6ff; margin-bottom: 8px; font-size: 14px; }
  #detail .field { margin-bottom: 4px; }
  #detail .label { color: #8b949e; }
  #detail .value { color: #c9d1d9; }
  .legend { position: fixed; bottom: 8px; left: 8px; background: #161b22; border: 1px solid #30363d; border-radius: 6px; padding: 8px 12px; font-size: 11px; z-index: 10; }
  .legend-item { display: flex; align-items: center; gap: 6px; margin: 2px 0; }
  .legend-dot { width: 10px; height: 10px; border-radius: 50%; }
</style>
</head>
<body>
<div id="toolbar">
  <h1>Rocky DAG Explorer</h1>
  <input type="text" id="search" placeholder="Search models..." autocomplete="off">
  <span id="stats"></span>
</div>
<div id="graph"></div>
<div id="detail">
  <h3 id="detail-name"></h3>
  <div class="field"><span class="label">Strategy: </span><span class="value" id="detail-strategy"></span></div>
  <div class="field"><span class="label">Layer: </span><span class="value" id="detail-layer"></span></div>
  <div class="field"><span class="label">Dependencies: </span><span class="value" id="detail-deps"></span></div>
  <div class="field"><span class="label">Dependents: </span><span class="value" id="detail-dependents"></span></div>
  <div class="field"><span class="label">Columns: </span><span class="value" id="detail-columns"></span></div>
</div>
<div class="legend">
  <div class="legend-item"><div class="legend-dot" style="background:#58a6ff"></div> Source</div>
  <div class="legend-item"><div class="legend-dot" style="background:#3fb950"></div> Staging</div>
  <div class="legend-item"><div class="legend-dot" style="background:#d29922"></div> Intermediate</div>
  <div class="legend-item"><div class="legend-dot" style="background:#f85149"></div> Marts</div>
</div>
<script src="https://d3js.org/d3.v7.min.js"></script>
<script>
const COLORS = { source: '#58a6ff', staging: '#3fb950', intermediate: '#d29922', marts: '#f85149', default: '#8b949e' };

function inferLayer(name) {
  if (name.startsWith('src_') || name.startsWith('raw_')) return 'source';
  if (name.startsWith('stg_') || name.startsWith('staging_')) return 'staging';
  if (name.startsWith('int_') || name.startsWith('intermediate_')) return 'intermediate';
  if (name.startsWith('mart_') || name.startsWith('fct_') || name.startsWith('dim_')) return 'marts';
  return 'default';
}

async function main() {
  let data;
  try {
    const resp = await fetch('/api/dag');
    data = await resp.json();
  } catch (e) {
    // Demo data if API not available
    data = { models: [
      { name: 'src_orders', depends_on: [] },
      { name: 'stg_orders', depends_on: ['src_orders'] },
      { name: 'int_order_items', depends_on: ['stg_orders'] },
      { name: 'mart_revenue', depends_on: ['int_order_items'] }
    ]};
  }

  const nodes = data.models.map(m => ({
    id: m.name, layer: inferLayer(m.name),
    strategy: m.strategy || 'full_refresh',
    columns: m.columns || [], depends_on: m.depends_on || []
  }));
  const nodeMap = new Map(nodes.map(n => [n.id, n]));
  const links = [];
  nodes.forEach(n => {
    (n.depends_on || []).forEach(dep => {
      if (nodeMap.has(dep)) links.push({ source: dep, target: n.id });
    });
  });

  document.getElementById('stats').textContent = nodes.length + ' models, ' + links.length + ' edges';

  const width = window.innerWidth, height = window.innerHeight - 44;
  const svg = d3.select('#graph').append('svg').attr('viewBox', [0, 0, width, height]);
  const g = svg.append('g');
  svg.call(d3.zoom().scaleExtent([0.1, 4]).on('zoom', e => g.attr('transform', e.transform)));

  const sim = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(60))
    .force('charge', d3.forceManyBody().strength(-120))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('y', d3.forceY().strength(0.05));

  const link = g.append('g').selectAll('line').data(links).join('line').attr('class', 'link');
  const node = g.append('g').selectAll('g').data(nodes).join('g').attr('class', 'node')
    .call(d3.drag().on('start', (e,d) => { if(!e.active) sim.alphaTarget(0.3).restart(); d.fx=d.x; d.fy=d.y; })
    .on('drag', (e,d) => { d.fx=e.x; d.fy=e.y; }).on('end', (e,d) => { if(!e.active) sim.alphaTarget(0); d.fx=null; d.fy=null; }));

  node.append('circle').attr('r', 6).attr('fill', d => COLORS[d.layer] || COLORS.default).attr('stroke', d => COLORS[d.layer] || COLORS.default);
  node.append('text').attr('dx', 10).attr('dy', 3).text(d => d.id);

  node.on('click', (e, d) => {
    const detail = document.getElementById('detail');
    detail.style.display = 'block';
    document.getElementById('detail-name').textContent = d.id;
    document.getElementById('detail-strategy').textContent = d.strategy;
    document.getElementById('detail-layer').textContent = d.layer;
    document.getElementById('detail-deps').textContent = (d.depends_on || []).join(', ') || 'none';
    const dependents = nodes.filter(n => (n.depends_on||[]).includes(d.id)).map(n => n.id);
    document.getElementById('detail-dependents').textContent = dependents.join(', ') || 'none';
    document.getElementById('detail-columns').textContent = (d.columns || []).length + ' columns';
    // Highlight path
    d3.selectAll('.node').classed('highlighted', n => n.id===d.id || (d.depends_on||[]).includes(n.id) || dependents.includes(n.id));
    d3.selectAll('.link').classed('highlighted', l => l.source.id===d.id || l.target.id===d.id);
  });

  sim.on('tick', () => {
    link.attr('x1',d=>d.source.x).attr('y1',d=>d.source.y).attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);
    node.attr('transform', d => 'translate('+d.x+','+d.y+')');
  });

  // Search
  document.getElementById('search').addEventListener('input', e => {
    const q = e.target.value.toLowerCase();
    node.classed('highlighted', d => q && d.id.toLowerCase().includes(q));
    node.style('opacity', d => !q || d.id.toLowerCase().includes(q) ? 1 : 0.2);
    link.style('opacity', !q ? 0.6 : 0.1);
  });
}
main();
</script>
</body>
</html>"##;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_html_is_valid() {
        let html = dag_viz_html();
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Rocky DAG Explorer"));
        assert!(html.contains("d3.v7.min.js"));
        assert!(html.contains("/api/dag"));
    }

    #[test]
    fn test_html_not_empty() {
        let html = dag_viz_html();
        assert!(html.len() > 1000);
    }
}
