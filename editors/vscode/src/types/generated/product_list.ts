/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/product_list.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output of `rocky product list`.
 *
 * One row per product the project knows, sorted by name. A project with no `products/` directory and no records lists nothing; that is not an error.
 */
export interface ProductListOutput {
  command: string;
  /**
   * `products.len()`, repeated so a consumer can assert it received the whole list.
   */
  count: number;
  /**
   * Every product, sorted by name.
   */
  products: ProductListEntry[];
  version: string;
  [k: string]: unknown;
}
/**
 * One product's row in `rocky product list`.
 *
 * A projection of [`ProductStatusOutput`], built by the same function `product status` uses, so the two can never disagree. Missing values are `null`, never invented.
 */
export interface ProductListEntry {
  /**
   * The approval record, when one exists.
   */
  approval?: ProductApprovalOutput | null;
  /**
   * Number of committed artifacts whose bytes no longer match the manifest. `0` is clean.
   */
  artifact_problems: number;
  /**
   * The committed lowering phase, when a manifest exists.
   */
  committed_phase?: string | null;
  /**
   * The persisted fulfillment state tag, when one exists.
   */
  fulfill_state?: string | null;
  /**
   * Number of fulfillment journal rows recorded for this product.
   */
  journal_rows: number;
  /**
   * Product name: the spec file's stem, or the state store's key.
   */
  name: string;
  /**
   * The resolved output model, when the spec parses.
   */
  output_model?: string | null;
  /**
   * `product:<name>`, when the spec parses.
   */
  product_id?: string | null;
  /**
   * The working spec's digest, when it parses.
   */
  spec_digest?: string | null;
  /**
   * Why the spec failed to load — absent or unparseable — when it did.
   */
  spec_error?: string | null;
  /**
   * Whether the working spec equals the approved revision. `null` when either side is missing.
   */
  spec_matches_approval?: boolean | null;
  /**
   * Whether `products/<name>.toml` exists and parses.
   */
  spec_present: boolean;
  /**
   * Whether an uncommitted staging journal is pending.
   */
  staging_journal_present: boolean;
  [k: string]: unknown;
}
/**
 * The approval record, as echoed by compile/status output.
 */
export interface ProductApprovalOutput {
  /**
   * RFC3339 approval instant.
   */
  approved_at?: string | null;
  /**
   * Best-effort approver identity.
   */
  approver: string;
  /**
   * Project-root-relative path of the immutable snapshot file.
   */
  snapshot_path: string;
  /**
   * The approved `sha256:<hex>` spec digest.
   */
  spec_digest: string;
  [k: string]: unknown;
}
