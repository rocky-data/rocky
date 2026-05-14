/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/apply.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky apply <plan-id>`.
 *
 * Wraps the inner apply output (compact / archive / run) with a top-level `plan_id` envelope so consumers can correlate the apply result back to the plan that generated it without examining the inner payload's command field.
 *
 * ## Shape decision: envelope (not discriminated enum)
 *
 * A discriminated enum with `CompactApplyOutput | ArchiveApplyOutput | RunOutput` would produce noisier `JsonSchema` derivations (nested `oneOf` with overlapping field names). An envelope with `inner: serde_json::Value` is simpler and lets consumers fall back to the per-command schema they already know for the `command` field (`"compact apply"` / `"archive apply"` / `"run"`).
 */
export interface ApplyOutput {
  /**
   * Always `"apply"` — the top-level command selector in `parse_rocky_output`.
   */
  command: string;
  /**
   * The plan that was applied (full 64-char blake3 hex).
   */
  plan_id: string;
  /**
   * `PlanKind` wire name: `"compact"`, `"archive"`, or `"run"`.
   */
  plan_kind: string;
  /**
   * The full inner apply result, embedded verbatim. Shape depends on `plan_kind`: - `"compact"` → `CompactApplyOutput` - `"archive"` → `ArchiveApplyOutput` - `"run"`     → `RunOutput`
   */
  result: {
    [k: string]: unknown;
  };
  /**
   * Whether all statements / materializations succeeded.
   */
  success: boolean;
  version: string;
  [k: string]: unknown;
}
