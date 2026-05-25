// Lineage: open a model and render its lineage as an interactive DAG webview
// (rocky lineage -> DOT -> SVG via viz.js). Exercises the harness's ability to
// capture a webview panel, not just the editor. Deterministic, offline.
//
// TODO: graph-fit-in-narrow-canvas needs more work. The webview opens in a
// split, leaving the SVG canvas cramped (~39% zoom). Maximizing the editor
// group reloads the webview, and a post-reload programmatic fit ('f' hotkey)
// doesn't reliably refit the SVG. This is content-polish, not a harness
// blocker — the panel, controls, and node counts all capture correctly.

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
  },
};
