/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/compliance.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * JSON output for `rocky compliance`.
 *
 * A thin rollup over Wave A governance: classification sidecars (`[classification]` per model) + project-level `[mask]` / `[mask.<env>]` policy. Answers the question **"are all classified columns masked wherever policy says they should be?"** without making any warehouse calls — purely a static resolver over `rocky.toml` + model sidecars.
 *
 * Consumers (CI gates, dagster) dispatch on the top-level `command` field (`"compliance"`).
 */
export interface ComplianceOutput {
  /**
   * Always `"compliance"`. Consumers key dispatch off this field.
   */
  command: string;
  /**
   * The unmasked-where-expected list. An exception fires when a classification tag has no resolved masking strategy **and** the tag is not on the project-level `[classifications] allow_unmasked` advisory list.
   */
  exceptions: ComplianceException[];
  /**
   * One entry per `(model, column)` pair carrying a classification tag. When `--exceptions-only` is set, this list is filtered to only the pairs whose `envs` contain at least one exception.
   */
  per_column: ColumnClassificationStatus[];
  /**
   * Aggregate tallies over the full `per_column` / `exceptions` lists.
   */
  summary: ComplianceSummary;
  /**
   * `CARGO_PKG_VERSION` at the time this payload was emitted.
   */
  version: string;
  [k: string]: unknown;
}
/**
 * A single compliance exception — a classified column with no resolved masking strategy in some environment, and whose classification tag is **not** on the advisory allow list.
 */
export interface ComplianceException {
  column: string;
  env: string;
  model: string;
  /**
   * Human-readable explanation. Always of the shape `"no masking strategy resolves for classification tag '<tag>'"` in v1; future variants (e.g., adapter-specific violations) may widen this.
   */
  reason: string;
  [k: string]: unknown;
}
/**
 * Per-`(model, column)` report: the classification tag and the resolved masking status across every evaluated environment.
 */
export interface ColumnClassificationStatus {
  /**
   * Free-form classification tag (e.g., `"pii"`, `"confidential"`). The engine does not enum-constrain these — projects coin tags as needed.
   */
  classification: string;
  /**
   * Column name — the key under the model's `[classification]` block.
   */
  column: string;
  /**
   * One entry per evaluated environment. When `--env X` is set, this is a single-element list labeled `"X"`. When unset, it expands across the union of the defaults (labeled `"default"`) and every named `[mask.<env>]` override block.
   */
  envs: EnvMaskingStatus[];
  /**
   * Model name — matches `ModelConfig::name` (the `.toml` sidecar key or the `.sql`/`.rocky` filename stem).
   */
  model: string;
  [k: string]: unknown;
}
/**
 * Masking status for one `(model, column, env)` triple.
 */
export interface EnvMaskingStatus {
  /**
   * `true` iff `masking_strategy != "unresolved"`. Allow-listed tags (on `[classifications] allow_unmasked`) still report `enforced = false` — the allow list only suppresses the [`ComplianceException`] emission, not the underlying fact that no strategy resolved.
   */
  enforced: boolean;
  /**
   * Environment label. `"default"` when the row came from the unscoped `[mask]` block; otherwise the name of the matching `[mask.<env>]` override, or the raw `--env` value.
   */
  env: string;
  /**
   * Resolved masking strategy. One of `"hash"`, `"redact"`, `"partial"`, `"none"`, or `"unresolved"` when no strategy applies to this classification tag in this env.
   */
  masking_strategy: string;
  [k: string]: unknown;
}
/**
 * Aggregate counters for a compliance report.
 *
 * All three counters are over `(model, column, env)` triples — so a single classified column evaluated across three envs contributes up to three to `total_classified`. This matches the granularity of `total_exceptions` and `total_masked`, keeping the invariant `total_classified = total_masked + total_exceptions + <allow_listed unresolved>` (the third term is not counted in either bucket, so it surfaces implicitly as the gap).
 */
export interface ComplianceSummary {
  /**
   * Total `(model, column, env)` triples across every classified column in the project, expanded across the env enumeration.
   */
  total_classified: number;
  /**
   * Triples where no masking strategy resolved and the classification tag is **not** on the `allow_unmasked` advisory list. Matches the length of [`ComplianceOutput::exceptions`].
   */
  total_exceptions: number;
  /**
   * Triples where a masking strategy successfully resolved (`enforced = true`). `MaskStrategy::None` ("explicit identity — no masking") counts as masked here because the project has deliberately opted out, which is a conscious policy decision rather than an enforcement gap.
   */
  total_masked: number;
  [k: string]: unknown;
}
