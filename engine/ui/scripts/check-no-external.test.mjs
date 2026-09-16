import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { externalLoads, layoutProblems } from "./check-no-external.mjs";

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
