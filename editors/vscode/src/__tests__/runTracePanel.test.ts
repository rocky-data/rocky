import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => ({}));

import { barGeometry, formatMs } from "../webviews/runTracePanel";

describe("formatMs", () => {
  it("formats sub-second, second, and minute durations", () => {
    expect(formatMs(340)).toBe("340ms");
    expect(formatMs(1200)).toBe("1.2s");
    expect(formatMs(65_000)).toBe("1m 5s");
  });
});

describe("barGeometry", () => {
  it("places a bar by its offset and duration", () => {
    expect(barGeometry(500, 250, 1000)).toEqual({ leftPct: 50, widthPct: 25 });
  });
  it("spans the full track when it covers the whole run", () => {
    expect(barGeometry(0, 1000, 1000)).toEqual({ leftPct: 0, widthPct: 100 });
  });
  it("floors width so tiny models stay visible", () => {
    const g = barGeometry(0, 0, 1000);
    expect(g.widthPct).toBe(0.5);
  });
  it("falls back to full width when total duration is zero", () => {
    expect(barGeometry(0, 0, 0)).toEqual({ leftPct: 0, widthPct: 100 });
  });
  it("never overflows the track", () => {
    const g = barGeometry(900, 500, 1000);
    expect(g.leftPct + g.widthPct).toBeLessThanOrEqual(100);
  });
});
