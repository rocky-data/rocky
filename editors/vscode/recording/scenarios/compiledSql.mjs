// Compiled SQL: open a Rocky DSL model, then reveal its macro-expanded compiled
// SQL in a side-by-side read-only document. Sells the core "typed DSL -> SQL"
// story. Fully local/deterministic (rocky compile --expand-macros), no
// warehouse, no webview (a virtual document, so no compositor risk).

export default {
  name: "compiledSql",
  description: "Open a model and reveal its compiled SQL.",
  workspace: "examples/playground/pocs/00-foundations/00-playground-default",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 1000,
  quality: 65,

  async run(d) {
    await d.openFile("customer_orders.rocky");
    await d.pause(3000);

    // Open Compiled SQL → rocky-compiled:/customer_orders.sql (read-only).
    await d.command("Rocky: Open Compiled SQL");
    await d.pause(3500);
  },
};
