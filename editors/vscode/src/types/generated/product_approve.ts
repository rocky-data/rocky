/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/product_approve.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky product approve`.
 */
export interface ProductApproveOutput {
  /**
   * True when this exact digest was already approved and nothing was re-written.
   */
  already_approved: boolean;
  /**
   * RFC3339 approval instant.
   */
  approved_at: string;
  /**
   * Best-effort approver identity recorded on the approval.
   */
  approver: string;
  command: string;
  /**
   * The resolved output model name.
   */
  output_model: string;
  /**
   * The fulfillment state tag before this approval, if any.
   */
  previous_state?: string | null;
  /**
   * `product:<name>`.
   */
  product_id: string;
  /**
   * Project-root-relative path of the immutable snapshot file.
   */
  snapshot_path: string;
  /**
   * The approved `sha256:<hex>` spec digest.
   */
  spec_digest: string;
  /**
   * The fulfillment state after this approval (`spec_approved`).
   */
  state: string;
  version: string;
  [k: string]: unknown;
}
