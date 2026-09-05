/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/model_detail.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Rocky's unified column type.
 *
 * Derives `JsonSchema` because it is served verbatim on the HTTP model detail route (`TypedColumnOutput::data_type`): the structured form is the only lossless one — the `Display` rendering drops a struct field's nullability.
 *
 * # Wire contract
 *
 * The serde shape — externally tagged, with these exact variant names — is published in `schemas/model_detail.schema.json` and the generated Python and TypeScript bindings. From engine 1.74.0 it is a public contract:
 *
 * - **Adding** a variant is additive. Consumers must treat an unknown tag as "a type this build does not know", never as an error. - **Renaming or removing** a variant, or changing a variant's payload, is a breaking change to the model-detail route and every binding. It needs a `Changed` changelog entry that names the old and new tags, and the codegen cascade in the same PR.
 *
 * The `Display` rendering (`data_type_display`) is a label and carries no such promise.
 */
export type RockyType =
  | ("Boolean" | "Int32" | "Int64" | "Float32" | "Float64" | "String" | "Date" | "Binary" | "Variant" | "Unknown")
  | {
      Decimal: {
        precision: number;
        scale: number;
        [k: string]: unknown;
      };
    }
  | "Timestamp"
  | "TimestampNtz"
  | {
      Array: RockyType;
    }
  | {
      /**
       * @minItems 2
       * @maxItems 2
       */
      Map: [RockyType, RockyType];
    }
  | {
      Struct: StructField[];
    };

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
   * Type-checked columns, or `null` when the type checker produced none for this model. Each carries the structured type and its label.
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
   * The inferred type, structured and lossless — a struct field keeps its own nullability, which the display string drops.
   */
  data_type: RockyType;
  /**
   * Rocky's human rendering of `data_type`, e.g. `INT64`, `DECIMAL(10,2)` or `STRUCT<a:INT64>`. A label, not a parser input.
   */
  data_type_display: string;
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
/**
 * A field in a struct type.
 */
export interface StructField {
  data_type: RockyType;
  name: string;
  nullable: boolean;
  [k: string]: unknown;
}
