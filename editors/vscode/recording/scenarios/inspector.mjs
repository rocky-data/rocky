// Inspector: open a model in the Rocky Inspector (bottom panel) and tour the
// whole dashboard — Overview, Columns, the interactive lineage canvas (with a
// trust-plane overlay), Tests, Preview, and Profile. This is the consolidated
// demo: it replaces the old separate `inspector` (Overview/Columns only) and
// `lineage` scenarios.
//
// The focal model `customer_360` is PII-classified, so the Overview's
// Governance card flags it ("2 classified / 1 unmasked"); it sits in a real
// neighborhood (3 upstream, 2 downstream) so the Lineage tab has something to
// show. The data tabs (Preview/Profile) and declarative Tests run `rocky`
// against the warehouse, so `setup()` materializes a fresh DuckDB first.

import { execSync } from "node:child_process";

export default {
  name: "inspector",
  description: "Tour the Rocky Inspector — trust dashboard, columns, lineage, tests, preview, profile.",
  workspace: "editors/vscode/recording/fixtures/inspector-showcase",
  size: { width: 1280, height: 800 },
  fps: 15,
  gifWidth: 920,
  quality: 50,

  // Materialize a FRESH poc.duckdb before launch so the data-backed tabs show
  // real rows, not spinners. Uses the same PATH `rocky` the launched VS Code
  // inherits, and rebuilds from scratch each run for a deterministic record (a
  // stale duckdb leaks old stats into the Columns/Profile tabs).
  async setup(workspace) {
    const sh = (cmd) =>
      execSync(cmd, {
        cwd: workspace,
        stdio: "inherit",
        env: { ...process.env, ROCKY_SUPPRESS_DEPRECATION: "1" },
      });
    sh("rm -f poc.duckdb .rocky-state.redb");
    sh("duckdb poc.duckdb < data/seed.sql");
    sh("rocky run");
  },

  async run(d) {
    await d.openFile("customer_360.sql", { settle: 450 });
    await d.pause(500); // a beat on the model

    await d.command("Rocky: Open in Inspector", { settle: 450 });
    await d.pause(700);
    await d.command("View: Toggle Maximized Panel", { settle: 450 });
    // catalog + compile + compliance fan-in; the Governance card lands and the
    // Overview trust dashboard lights up (the default tab).
    await d.pause(3200);

    // Tour the tabs. "text=Overview" locates the Inspector iframe — the tab bar
    // shows every label on every tab, so the marker stays valid throughout. The
    // low click-settle just registers the switch; the pause after is the dwell
    // (DuckDB-local round-trips are sub-second, so the data lands well within).
    await d.clickInWebview("text=Overview", "Columns", { settle: 250 });
    await d.pause(1900);

    await d.clickInWebview("text=Overview", "Lineage", { settle: 250 });
    await d.pause(2600); // catalog + compile, dagre layout, fitView
    await d.clickInWebview(".react-flow", "Cost", { settle: 250 }); // light up an overlay
    await d.pause(1900);

    await d.clickInWebview("text=Overview", "Tests", { settle: 250 });
    await d.pause(2100);

    await d.clickInWebview("text=Overview", "Preview", { settle: 250 });
    await d.pause(2100);

    await d.clickInWebview("text=Overview", "Profile", { settle: 250 });
    await d.pause(2300);

    // End back on the trust dashboard.
    await d.clickInWebview("text=Overview", "Overview", { settle: 250 });
    await d.pause(1500);
  },
};
