// Lineage: open a model and render its lineage as an interactive DAG webview
// (rocky lineage -> DOT -> SVG via viz.js). Exercises the harness's ability to
// capture a webview panel, not just the editor. Deterministic, offline.

export default {
  name: "lineage",
  description: "Render a model's lineage DAG in the webview.",
  // Self-contained POC: fct_revenue <- stg_orders <- raw_orders.
  workspace: "examples/playground/pocs/06-developer-experience/01-lineage-column-level",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    await d.openFile("fct_revenue.rocky");
    await d.pause(2500);

    // Show Model Lineage opens a webview panel for the active model.
    await d.command("Rocky: Show Model Lineage");
    await d.pause(4500); // CLI lineage call + DOT->SVG render

    // NOTE: kept on the split view on purpose. The graph now re-fits on resize
    // (ResizeObserver fix in src/commands/lineage.ts, covered by e2e/lineage.spec.mjs),
    // but the maximized SVG doesn't reliably repaint under the recording-time
    // compositor, so the wider view records blank. The fit fix is real (verified
    // via the webview DOM in the e2e test); this is a recording artifact only.
  },
};
