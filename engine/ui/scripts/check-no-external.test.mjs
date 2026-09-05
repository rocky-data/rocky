import { describe, expect, it } from "vitest";
import { externalLoads } from "./check-no-external.mjs";

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
