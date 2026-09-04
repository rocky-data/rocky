/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/model_detail.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * One model's detail for `GET /api/v1/models/{name}`.
 */
export interface ModelDetailOutput {
  /**
   * Inferred output columns.
   */
  columns: ModelColumnOutput[];
  /**
   * Direct downstream model names.
   */
  downstream: string[];
  /**
   * Path to the source file, as the compiler recorded it.
   */
  file_path: string;
  /**
   * Whether the model uses `SELECT *`, so its schema depends on upstream.
   */
  has_star: boolean;
  /**
   * Model name.
   */
  name: string;
  /**
   * The model's SQL text, capped at [`MODEL_DETAIL_SQL_CAP_BYTES`] bytes. When it was cut, `sql_truncated` is `true` and `sql_bytes` carries the full length, so a consumer never mistakes a cut text for the whole.
   */
  sql: string;
  /**
   * Length of the full SQL text in bytes, cut or not.
   */
  sql_bytes: number;
  /**
   * Whether `sql` was cut at the cap.
   */
  sql_truncated: boolean;
  /**
   * Type-checked columns, or `null` when the type checker produced none for this model.
   */
  typed_columns?: TypedColumnOutput[] | null;
  /**
   * Direct upstream model names.
   */
  upstream: string[];
  [k: string]: unknown;
}
/**
 * One inferred output column of a model.
 */
export interface ModelColumnOutput {
  /**
   * Column name.
   */
  name: string;
  [k: string]: unknown;
}
/**
 * One type-checked column of a model.
 */
export interface TypedColumnOutput {
  /**
   * Rocky's rendering of the inferred type, e.g. `INT64` or `DECIMAL(10,2)`.
   */
  data_type: string;
  /**
   * Column name.
   */
  name: string;
  /**
   * Whether the column may be `NULL`.
   */
  nullable: boolean;
  [k: string]: unknown;
}
