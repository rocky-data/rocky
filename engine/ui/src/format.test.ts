import { describe, expect, it } from "vitest";
import { NOT_RECORDED, formatDuration, formatInstant, orNotRecorded, shortId } from "./format";

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

describe("shortId", () => {
  it("keeps short ids and cuts long ones to twelve characters", () => {
    expect(shortId("run-1")).toBe("run-1");
    expect(shortId("a".repeat(64))).toBe("a".repeat(12));
  });
});
