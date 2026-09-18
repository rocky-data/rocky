import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { externalLoads, layoutProblems, shellLoadProblems } from "./check-no-external.mjs";

describe("layoutProblems", () => {
  const build = (files) => {
    const dir = mkdtempSync(join(tmpdir(), "rocky-dist-"));
    for (const f of files) {
      mkdirSync(join(dir, f, ".."), { recursive: true });
      writeFileSync(join(dir, f), "");
    }
    return dir;
  };

  it("accepts index.html at the root and everything else under assets/", () => {
    const dir = build(["index.html", "assets/index-abc.js", "assets/index-abc.css"]);
    expect(layoutProblems(dir)).toHaveLength(0);
  });

  it("refuses a root-level file other than index.html, and a second top-level directory", () => {
    // The server answers the shell for any non-file path outside assets/, so
    // a root-level bundle would be served as HTML when its hash goes stale.
    expect(layoutProblems(build(["index.html", "index-abc.js"]))).toHaveLength(1);
    expect(layoutProblems(build(["index.html", "static/a.js"]))).toHaveLength(1);
  });
});

describe("shellLoadProblems", () => {
  const shell = (html, assets = ["index-abc.js", "index-abc.css"]) => {
    const dir = mkdtempSync(join(tmpdir(), "rocky-shell-"));
    mkdirSync(join(dir, "assets"), { recursive: true });
    for (const name of assets) writeFileSync(join(dir, "assets", name), "");
    writeFileSync(join(dir, "index.html"), html);
    return dir;
  };

  it("accepts the hrefs Vite emits, and a data URI", () => {
    const dir = shell(
      `<link rel="icon" href="/ui/assets/rocky-logo-abc.svg">
       <link rel="stylesheet" href="/ui/assets/index-abc.css">
       <script type="module" src="/ui/assets/index-abc.js"></script>
       <link rel="apple-touch-icon" href="data:image/svg+xml,%3csvg%3e">`,
      ["index-abc.js", "index-abc.css", "rocky-logo-abc.svg"],
    );
    expect(shellLoadProblems(dir)).toHaveLength(0);
  });

  it("refuses a source path the build failed to rewrite", () => {
    // The tab icon is written as `./src/assets/rocky-logo.svg`. If Vite ever
    // stops rewriting it, the server answers `/ui/src/...` with the shell, so
    // the icon would be an HTML document and would not draw — silently.
    const dir = shell('<link rel="icon" href="./src/assets/rocky-logo.svg">');
    expect(shellLoadProblems(dir)).toHaveLength(1);
  });

  it("refuses an asset href with no file behind it", () => {
    const dir = shell('<link rel="icon" href="/ui/assets/rocky-logo-stale.svg">');
    expect(shellLoadProblems(dir)).toEqual([
      'index.html: href="/ui/assets/rocky-logo-stale.svg" has no file in assets/',
    ]);
  });
});

describe("externalLoads", () => {
  it("flags a script, a stylesheet, a font and a fetch from another host", () => {
    expect(externalLoads('<script src="https://cdn.example/d3.js">')).toHaveLength(1);
    expect(externalLoads('<link href="https://fonts.example/x.css">')).toHaveLength(1);
    expect(externalLoads("@font-face{src:url(https://fonts.example/a.woff2)}")).toHaveLength(1);
    expect(externalLoads('fetch("https://api.example/x")')).toHaveLength(1);
    expect(externalLoads('import("https://esm.example/x.js")')).toHaveLength(1);
  });

  it("ignores same-origin loads and URLs that are only text", () => {
    expect(externalLoads('<script type="module" src="/ui/assets/index-abc.js">')).toHaveLength(0);
    expect(externalLoads('const docs = "https://rocky-data.dev/guides/embedding/";')).toHaveLength(0);
    expect(externalLoads("url(/ui/assets/a.woff2)")).toHaveLength(0);
  });
});
