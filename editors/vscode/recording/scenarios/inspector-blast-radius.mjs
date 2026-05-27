// Blast-radius popover: open an upstream model in the Inspector and click the
// "N downstream" value on the Blast radius card to reveal exactly which models a
// change would hit. The DAG is raw_orders → stg_orders → fct_revenue, so opening
// raw_orders gives 2 downstream. The popover renders in the browser top layer
// (el-popover), so it floats over the panel rather than clipping at its edge.

export default {
  name: "inspector-blast-radius",
  description: "Click Blast radius to reveal which downstream models a change would hit.",
  workspace: "examples/playground/pocs/06-developer-experience/01-lineage-column-level",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    await d.openFile("raw_orders.sql");
    await d.pause(600);

    await d.command("Rocky: Open in Inspector");
    await d.pause(1500);
    await d.command("View: Toggle Maximized Panel");
    await d.pause(3000); // catalog + compile fan-in; Overview is the default tab

    // The Blast radius value is a button named "2 downstream" — click it to open
    // the top-layer popover listing the downstream models.
    await d.clickInWebview("text=Overview", "downstream");
    await d.pause(2800);
  },
};
