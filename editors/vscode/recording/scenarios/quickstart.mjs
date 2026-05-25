// Quickstart: open a Rocky model (syntax highlighting + inline CodeLens from
// the running LSP), then reveal the Rocky command surface. No warehouse calls,
// no webview, no mouse — fully keyboard-driven and offline.

export default {
  name: "quickstart",
  description: "Open a Rocky model and reveal the Rocky command palette.",
  workspace: "examples/playground",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    // A model with Run/Compile/Test/Lineage CodeLens above it.
    await d.openFile("customer_orders.rocky");
    await d.pause(3000);

    // Show the branded command surface.
    await d.palette("Rocky: ");
    await d.pause(3000);
    await d.escape();
    await d.pause(700);
  },
};
