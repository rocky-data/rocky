import { describe, expect, it } from "vitest";
import {
  NOT_RECORDED,
  clipHead,
  clipMiddle,
  formatDuration,
  formatInstant,
  orNotRecorded,
} from "./format";

describe("orNotRecorded", () => {
  it("renders null, undefined and the empty string as the status, and values as text", () => {
    expect(orNotRecorded(null)).toBe(NOT_RECORDED);
    expect(orNotRecorded(undefined)).toBe(NOT_RECORDED);
    expect(orNotRecorded("")).toBe(NOT_RECORDED);
    expect(orNotRecorded(0)).toBe("0");
    expect(orNotRecorded("cron")).toBe("cron");
  });
});

describe("formatDuration", () => {
  it("picks the unit by size", () => {
    expect(formatDuration(45)).toBe("45ms");
    expect(formatDuration(1234)).toBe("1.2s");
    expect(formatDuration(184_000)).toBe("3m 04s");
    expect(formatDuration(7_500_000)).toBe("2h 05m");
    expect(formatDuration(-1)).toBe(NOT_RECORDED);
  });
});

describe("formatInstant", () => {
  it("renders an RFC 3339 instant in UTC, with how long ago when now is known", () => {
    const iso = "2026-09-05T08:00:00Z";
    expect(formatInstant(iso)).toBe("2026-09-05 08:00:00 UTC");
    expect(formatInstant(iso, Date.parse("2026-09-05T08:03:00Z"))).toBe(
      "2026-09-05 08:00:00 UTC (3 min ago)",
    );
    expect(formatInstant(iso, Date.parse("2026-09-07T08:00:00Z"))).toBe(
      "2026-09-05 08:00:00 UTC (2 d ago)",
    );
  });

  it("renders an absent instant as the status and an unparseable one as given", () => {
    expect(formatInstant(null)).toBe(NOT_RECORDED);
    expect(formatInstant(undefined)).toBe(NOT_RECORDED);
    expect(formatInstant("soon")).toBe("soon");
  });
});

describe("clipHead", () => {
  it("keeps short ids whole and says when it cut", () => {
    expect(clipHead("run-1")).toEqual({ clipped: false, text: "run-1" });
    expect(clipHead("a".repeat(64))).toEqual({ clipped: true, head: "a".repeat(12), tail: "" });
  });

  it("does not claim a cut it did not make", () => {
    // Exactly at the limit: twelve characters are shown, nothing was dropped,
    // so a marker would claim a truncation that did not happen.
    expect(clipHead("a".repeat(12))).toEqual({ clipped: false, text: "a".repeat(12) });
    expect(clipHead("a".repeat(13)).clipped).toBe(true);
  });

  /// `String.prototype.slice` counts UTF-16 code units, and a cut at the
  /// twelfth unit of `aaaaaaaaaaa😀z` lands between the emoji's two
  /// surrogates, leaving a lone `\ud83d` on screen (#1815). The cut is made
  /// between characters a reader sees.
  it("never splits a surrogate pair", () => {
    const cut = clipHead("aaaaaaaaaaa😀z");
    expect(cut).toEqual({ clipped: true, head: "aaaaaaaaaaa😀", tail: "" });
    expect(/[\ud800-\udbff]$/.test(cut.clipped ? cut.head : "")).toBe(false);
  });

  /// The marker is a flag, not a character, so a value that contains "…"
  /// is not mistaken for one that was cut.
  it("reports a literal ellipsis as text that was not cut", () => {
    expect(clipHead("abcdefghij…")).toEqual({ clipped: false, text: "abcdefghij…" });
  });
});

describe("clipMiddle", () => {
  it("keeps both ends of a compound identifier", () => {
    // #1756: a leading slice renders every key for one product identically,
    // because they all begin `product:<name>@`. The tail is what
    // distinguishes them.
    const key = "product:revenue_daily@sha256:5b1bf5c@21";
    expect(clipMiddle(key)).toEqual({ clipped: true, head: "product:re", tail: "1bf5c@21" });
  });

  it("distinguishes two keys whose heads are the same", () => {
    const a = clipMiddle("product:revenue_daily@sha256:5b1bf5c@21");
    const b = clipMiddle("product:revenue_daily@sha256:5b1bf5c@22");
    expect(clipHead("product:revenue_daily@sha256:5b1bf5c@21")).toEqual(
      clipHead("product:revenue_daily@sha256:5b1bf5c@22"),
    );
    expect(a).not.toEqual(b);
  });

  it("returns a value uncut rather than rendering it longer", () => {
    // head + tail + the marker is 19 characters, so anything at or under
    // that gains nothing from cutting.
    expect(clipMiddle("a".repeat(19))).toEqual({ clipped: false, text: "a".repeat(19) });
    const cut = clipMiddle("a".repeat(20));
    expect(cut.clipped).toBe(true);
    expect(clipMiddle("short")).toEqual({ clipped: false, text: "short" });
  });

  it("never splits a surrogate pair at either end", () => {
    const cut = clipMiddle("aaaaaaaaa😀bbbbbbbbbb");
    expect(cut).toEqual({ clipped: true, head: "aaaaaaaaa😀", tail: "bbbbbbbb" });
  });
});
