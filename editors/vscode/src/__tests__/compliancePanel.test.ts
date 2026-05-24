import { describe, expect, it, vi } from "vitest";

vi.mock("vscode", () => ({}));

import {
  complianceVerdict,
  formatEnvStatuses,
} from "../webviews/compliancePanel";

describe("complianceVerdict", () => {
  it("ok when every classified column resolves to a strategy", () => {
    expect(
      complianceVerdict({
        total_classified: 3,
        total_masked: 3,
        total_exceptions: 0,
      }),
    ).toBe("ok");
  });

  it("fail when there are exceptions", () => {
    expect(
      complianceVerdict({
        total_classified: 3,
        total_masked: 1,
        total_exceptions: 2,
      }),
    ).toBe("fail");
  });

  it("warn when unmasked columns are allow-listed (the gap)", () => {
    // 3 classified, 1 masked, 0 exceptions → 2 allow-listed unresolved.
    expect(
      complianceVerdict({
        total_classified: 3,
        total_masked: 1,
        total_exceptions: 0,
      }),
    ).toBe("warn");
  });
});

describe("formatEnvStatuses", () => {
  it("joins env statuses and flags unenforced ones", () => {
    expect(
      formatEnvStatuses([
        { env: "default", masking_strategy: "hash", enforced: true },
        { env: "prod", masking_strategy: "unresolved", enforced: false },
      ]),
    ).toBe("default: hash · prod: unresolved ⚠");
  });

  it("handles a single env", () => {
    expect(
      formatEnvStatuses([
        { env: "default", masking_strategy: "redact", enforced: true },
      ]),
    ).toBe("default: redact");
  });
});
