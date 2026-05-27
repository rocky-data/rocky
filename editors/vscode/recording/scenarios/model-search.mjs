// Model search: open the Rocky Inspector, maximize the panel, then open the
// model-search command palette (TailwindPlus Elements), type to filter, and
// pick a model — re-targeting the Inspector. Opened via the header button: a
// keyboard-only driver can't reliably send Cmd+K past VS Code's chord prefix,
// so the button is the stable trigger (Cmd+K still works for users in-editor).

export default {
  name: "model-search",
  description: "Search models with the command palette and re-target the Inspector.",
  workspace: "examples/playground/pocs/06-developer-experience/01-lineage-column-level",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    await d.openFile("fct_revenue.rocky");
    await d.pause(600);

    await d.command("Rocky: Open in Inspector");
    await d.pause(1500);
    // Bottom-panel Inspector — maximize so the palette has room.
    await d.command("View: Toggle Maximized Panel");
    await d.pause(3000); // catalog + compile fan-in

    // Open the model-search palette from the header button, then filter + pick.
    await d.clickInWebview("text=Search models", "Search models");
    await d.pause(1400); // palette open, every model listed
    await d.type("stg");
    await d.pause(1600); // filtered to stg_orders
    await d.key("Enter");
    await d.pause(2600); // the Inspector re-targets to the picked model
  },
};
