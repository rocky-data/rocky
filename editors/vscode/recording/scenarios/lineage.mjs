// Lineage: render the project as an interactive ReactFlow canvas — the Rocky
// Inspector's Lineage tab — focused on a model's neighborhood, then light up a
// trust-plane overlay on the graph. The canvas fans in `rocky catalog` + `rocky
// compile`. The Inspector opens in the bottom panel, so the scenario maximizes
// it to give the canvas room. Interactions use the webview-reach driver (no
// visible cursor — the GIF shows the badges appearing, not the click).

export default {
  name: "lineage",
  description: "Render the lineage canvas (Inspector tab) and light up an overlay.",
  // Self-contained POC: fct_revenue <- stg_orders <- raw_orders.
  workspace: "examples/playground/pocs/06-developer-experience/01-lineage-column-level",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    await d.openFile("fct_revenue.rocky");
    await d.pause(600); // openFile already settles ~2.5s; just a beat on the model

    // "Show Model Lineage" reveals the Inspector on its Lineage tab, framed on
    // the focal model's neighborhood. Maximize the panel so the canvas has room.
    await d.command("Rocky: Show Model Lineage");
    await d.pause(1500);
    await d.command("View: Toggle Maximized Panel");
    await d.pause(3500); // catalog + compile fan-in, dagre layout, fitView

    // Toggle an overlay so the GIF shows trust-plane data on the graph. Cost
    // rides on compile's heuristic estimate; swap for Freshness / Drift /
    // Breaking / Last run / Governance depending on which has data for the
    // recorded POC (Drift / Last run need a prior `rocky run`; Breaking needs a
    // git base ref; Governance needs classified columns).
    await d.clickInWebview(".react-flow", "Cost");
    await d.pause(2500);
  },
};
