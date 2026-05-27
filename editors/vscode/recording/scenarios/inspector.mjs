// Inspector: open a model in the Catalog/Inspector panel and tour its tabs.
// The panel fans in `rocky catalog` + `rocky compile` on open. The Overview /
// Columns / Lineage tabs are catalog-fed and always render; Tests / Preview /
// Profile load lazily and need a materialized target (record against a POC
// you've `rocky run` to show those, then append their tab clicks below).

export default {
  name: "inspector",
  description: "Open a model in the Inspector and tour its tabs.",
  workspace: "examples/playground/pocs/06-developer-experience/01-lineage-column-level",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    await d.openFile("fct_revenue.rocky");
    await d.pause(600); // openFile already settles ~2.5s; just a beat on the model

    await d.command("Rocky: Open in Inspector");
    await d.pause(4000); // catalog + compile fan-in

    // Tour the catalog-fed tabs. "Overview" is always present, so it's the
    // stable marker for the Inspector webview frame; each tab is a button.
    await d.clickInWebview("text=Overview", "Columns");
    await d.pause(2200);
    await d.clickInWebview("text=Overview", "Lineage");
    await d.pause(2600);
  },
};
