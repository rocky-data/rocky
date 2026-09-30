import { describe, expect, it } from "vitest";
import { kindClass, kindGlyph } from "./ModelNode";
import { NODE_KINDS } from "./nodeRoute";

// The default dot and grey accent, unchanged from before #1859. Every other
// glyph and accent must differ from these, and from each other.
const DEFAULT_GLYPH = "•";
const DEFAULT_CLASS = "border-l-zinc-400";

// A kind the engine never sends. `ModelNode` used to key its table on this
// one, among others (`model`, `view`) — none of them a `NodeKind` variant.
const UNKNOWN_KIND = "materializedview";

describe("kindGlyph", () => {
  it.each(NODE_KINDS)("gives %s its own glyph, not the default dot", (kind) => {
    expect(kindGlyph(kind)).not.toBe(DEFAULT_GLYPH);
  });

  it("falls to the default dot for a kind the engine does not send", () => {
    expect(kindGlyph(UNKNOWN_KIND)).toBe(DEFAULT_GLYPH);
  });

  it("gives every engine kind a distinct glyph", () => {
    const glyphs = NODE_KINDS.map(kindGlyph);
    expect(new Set(glyphs).size).toBe(glyphs.length);
  });
});

describe("kindClass", () => {
  it.each(NODE_KINDS)("gives %s its own accent, not the default grey", (kind) => {
    expect(kindClass(kind)).not.toBe(DEFAULT_CLASS);
  });

  it("falls to the default grey accent for a kind the engine does not send", () => {
    expect(kindClass(UNKNOWN_KIND)).toBe(DEFAULT_CLASS);
  });

  it("gives every engine kind a distinct accent", () => {
    const classes = NODE_KINDS.map(kindClass);
    expect(new Set(classes).size).toBe(classes.length);
  });
});
