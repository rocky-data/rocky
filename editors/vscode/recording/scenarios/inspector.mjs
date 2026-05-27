// Inspector: open a model in the Rocky Inspector (bottom panel) and tour its
// tabs, including the Lineage tab's project canvas. The panel fans in `rocky
// catalog` + `rocky compile` on open. Overview / Columns / Lineage are
// catalog-fed and always render; Tests / Preview / Profile load lazily and need
// a materialized target. The Inspector opens in the bottom panel, so the
// scenario maximizes it (Cmd+J equivalent) to give the tabs and canvas room.

export default {
  name: "inspector",
  description: "Open a model in the Rocky Inspector and tour its tabs.",
  workspace: "examples/playground/pocs/06-developer-experience/01-lineage-column-level",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    await d.openFile("fct_revenue.rocky");
    await d.pause(600); // openFile already settles ~2.5s; just a beat on the model

    await d.command("Rocky: Open in Inspector");
    await d.pause(1500);
    // The Inspector lives in the bottom panel; maximize it so the tabs and the
    // Lineage tab's canvas have vertical room.
    await d.command("View: Toggle Maximized Panel");
    await d.pause(3000); // catalog + compile fan-in

    // Tour the catalog-fed tabs. "Overview" is always present, so it's the
    // stable marker for the Inspector webview frame; each tab is a button.
    await d.clickInWebview("text=Overview", "Columns");
    await d.pause(2200);
    await d.clickInWebview("text=Overview", "Lineage");
    await d.pause(2800); // the canvas mounts and fits into the maximized panel
  },
};
