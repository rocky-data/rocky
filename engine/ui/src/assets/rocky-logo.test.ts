import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";

/**
 * The UI's mark is the docs site's favicon, byte for byte.
 *
 * It is a copy rather than a reference because the two subprojects build
 * separately: the UI bundles its assets into the binary, and nothing may load
 * from another host. A copy drifts silently, so this compares the bytes.
 *
 * Paths come from the working directory (`engine/ui`, where vitest runs) and
 * are checked, so a runner started elsewhere fails here instead of passing on
 * a file it never read.
 */
function bytes(relative: string): Buffer {
  const path = resolve(process.cwd(), relative);
  if (!existsSync(path)) {
    throw new Error(`${path} does not exist; vitest ran from ${process.cwd()}, expected engine/ui`);
  }
  return readFileSync(path);
}

function read(relative: string): string {
  return bytes(relative).toString("utf8");
}

describe("the Rocky mark", () => {
  it("is the same file the docs site serves as its favicon", () => {
    // Bytes, not decoded text: two different malformed byte sequences decode
    // to the same replacement character, so a string compare could pass on a
    // copy that is not identical.
    const mine = bytes("src/assets/rocky-logo.svg");
    const docs = bytes("../../docs/public/favicon.svg");
    expect(mine.equals(docs)).toBe(true);
    expect(mine.length).toBe(docs.length);
  });

  it("carries its own size and needs no external font or image", () => {
    const svg = read("src/assets/rocky-logo.svg");
    expect(svg).toContain('viewBox="0 0 200 200"');
    // Nothing to fetch: no <image>, no @font-face, no url() reference. The
    // served page's CSP is `default-src 'self'`, so a mark that loaded
    // anything would simply not draw.
    expect(svg).not.toMatch(/<image|@font-face|url\(/);
  });

  it("is the icon the tab shows, from assets/ and not from the page root", () => {
    // `public/` would put a file at the root of `dist`, which breaks the
    // layout `rocky serve --ui` trusts (`scripts/check-no-external.mjs`).
    const html = read("index.html");
    expect(html).toContain('rel="icon"');
    expect(html).toContain('type="image/svg+xml"');
    expect(html).toContain("./src/assets/rocky-logo.svg");
  });
});
