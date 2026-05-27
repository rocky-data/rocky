import { describe, expect, it } from "vitest";
import {
  contractLabel,
  costLabel,
  formatCount,
  formatDuration,
  freshnessLabel,
  statusRank,
  testStatusByColumn,
  toStatus,
} from "./viewModel";

describe("viewModel", () => {
  it("labels contract sources", () => {
    expect(contractLabel("auto")).toBe("Auto (sidecar)");
    expect(contractLabel("explicit")).toBe("Explicit");
    expect(contractLabel(null)).toBe("None");
  });

  it("formats durations compactly", () => {
    expect(formatDuration(45)).toBe("45s");
    expect(formatDuration(120)).toBe("2m");
    expect(formatDuration(7200)).toBe("2h");
    expect(formatDuration(172800)).toBe("2d");
  });

  it("formats freshness with optional severity", () => {
    expect(freshnessLabel(null)).toBeNull();
    expect(freshnessLabel({ max_lag_seconds: 7200 })).toBe("≤ 2h");
    expect(freshnessLabel({ max_lag_seconds: 3600, severity: "error" })).toBe(
      "≤ 1h (error)",
    );
  });

  it("formats counts and the heuristic cost", () => {
    expect(formatCount(500)).toBe("500");
    expect(formatCount(1500)).toBe("1.5k");
    expect(formatCount(2_000_000)).toBe("2.0M");
    expect(costLabel(null)).toBeNull();
    expect(
      costLabel({
        confidence: "high",
        estimated_bytes: 0,
        estimated_cost_usd: 0.012,
        estimated_rows: 1500,
      }),
    ).toBe("~$0.0120 · 1.5k rows (high)");
  });

  it("maps a result's status + severity to a column status", () => {
    expect(toStatus("pass", "error")).toBe("pass");
    expect(toStatus("fail", "error")).toBe("fail");
    expect(toStatus("fail", "warning")).toBe("warn");
    expect(toStatus("error", "error")).toBe("fail");
  });

  it("reduces per-column status to the worst outcome", () => {
    const map = testStatusByColumn([
      { model: "m", table: "t", test_type: "not_null", column: "id", severity: "error", sql: "", status: "pass" },
      { model: "m", table: "t", test_type: "unique", column: "id", severity: "error", sql: "", status: "fail" },
      { model: "m", table: "t", test_type: "x", column: "name", severity: "warning", sql: "", status: "fail" },
      { model: "m", table: "t", test_type: "y", column: null, severity: "error", sql: "", status: "pass" },
    ]);
    expect(map.get("id")).toBe("fail"); // pass then fail → fail
    expect(map.get("name")).toBe("warn"); // fail + warning severity → warn
    expect(map.has("")).toBe(false); // null column is ignored
    expect(statusRank(map.get("id"))).toBe(3);
    expect(statusRank(undefined)).toBe(0);
  });
});
