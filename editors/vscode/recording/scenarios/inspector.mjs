// Inspector: open a governed model in the Rocky Inspector (bottom panel) and
// tour it. The Overview is a model trust dashboard — on this PII-classified
// model the Governance card lights red ("2 classified · 1 unmasked"), the kind
// of signal a SQL compiler has no engine to produce. The panel fans in `rocky
// catalog` + `rocky compile` (plus the compliance overlay) on open; it lives in
// the bottom panel, so the scenario maximizes it to give the dashboard room.

export default {
  name: "inspector",
  description: "Open a model in the Rocky Inspector — a model trust dashboard — and tour it.",
  workspace: "examples/playground/pocs/04-governance/05-classification-masking-compliance",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    await d.openFile("accounts.sql");
    await d.pause(700); // openFile settles ~2.5s; a beat on the model

    await d.command("Rocky: Open in Inspector");
    await d.pause(1500);
    await d.command("View: Toggle Maximized Panel");
    // catalog + compile fan-in, then the governance overlay lands and the
    // dashboard lights up — dwell on the lit Overview (the default tab).
    await d.pause(5200);

    // "Overview" is a stable marker for the Inspector webview frame; step into
    // Columns to show the per-column classification + upstream lineage.
    await d.clickInWebview("text=Overview", "Columns");
    await d.pause(2600);
  },
};
