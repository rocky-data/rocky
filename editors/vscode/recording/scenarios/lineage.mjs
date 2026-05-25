// Lineage: open a model and render its lineage DAG. As of the panel migration,
// "Show Model Lineage" renders in the bottom panel (full width) instead of a
// side editor tab, so the graph has horizontal room. Deterministic, offline.

export default {
  name: "lineage",
  description: "Render a model's lineage DAG in the bottom panel.",
  // Self-contained POC: fct_revenue <- stg_orders <- raw_orders.
  workspace: "examples/playground/pocs/06-developer-experience/01-lineage-column-level",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    await d.openFile("fct_revenue.rocky");
    await d.pause(2500);

    // Show Model Lineage reveals the Lineage panel (bottom, full width).
    await d.command("Rocky: Show Model Lineage");
    await d.pause(4500); // CLI lineage call + DOT->SVG render + ResizeObserver fit
  },
};
