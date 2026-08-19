/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/product_verify.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * The verdict a policy rule (or the default posture) yields.
 *
 * Ordered by restrictiveness for incomparable-rule tie-breaking: `Deny` is a hard override (handled separately), and among non-deny verdicts `RequireReview` is more restrictive than `Allow`.
 */
export type PolicyEffect = "allow" | "require_review" | "deny";
/**
 * Verification verdict, ordered by severity.
 */
export type VerifyStatus = "pass" | "needs_input" | "fail";

/**
 * JSON output of `rocky product verify`.
 */
export interface ProductVerifyOutput {
  /**
   * The resolved agent-apply effect, when evaluation ran.
   */
  apply_effect?: PolicyEffect | null;
  command: string;
  /**
   * The resolved output model name.
   */
  output_model: string;
  /**
   * The corrected `[policy]` block to paste, on a posture `needs_input`.
   */
  paste_block?: string | null;
  /**
   * `product:<name>`.
   */
  product_id: string;
  /**
   * The resolved agent-propose effect, when evaluation ran.
   */
  propose_effect?: PolicyEffect | null;
  /**
   * Why, in plain language.
   */
  reason: string;
  /**
   * `sha256:<hex>` over the spec's raw bytes.
   */
  spec_digest: string;
  /**
   * `pass`, `needs_input`, or `fail` — also the exit code (0 / 1 / 2).
   */
  status: VerifyStatus;
  version: string;
  [k: string]: unknown;
}
