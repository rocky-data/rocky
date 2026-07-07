/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/meta.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Feature-detection payload for `GET /api/v1/meta`.
 *
 * Fingerprints the running engine + bound config so an embedder can pin against a build without version-sniffing. Every field is computed at request time — none are baked literals — so `state_schema_version`, `schemas_hash`, and `config_hash` track the live engine and the on-disk config even across a long-running sidecar.
 */
export interface MetaOutput {
  /**
   * Feature/capability tokens the build advertises.
   */
  capabilities: string[];
  /**
   * Fingerprint of the resolved `rocky.toml` (contents + path) this sidecar bound, recomputed per request. `null` when no config resolved.
   */
  config_hash?: string | null;
  /**
   * Engine release (`CARGO_PKG_VERSION`).
   */
  engine_version: string;
  /**
   * The `/api/v1` routes this build serves.
   */
  routes: string[];
  /**
   * Fingerprint of the exported JSON-schema set (the `schemas/*.schema.json` contract). Derived from the live registered schemas, so it moves on every codegen; embedders detect an output-shape change without version-sniffing.
   */
  schemas_hash: string;
  /**
   * Current state-store schema version, read from the engine's `current_schema_version()` getter at request time (never a literal).
   */
  state_schema_version: number;
  [k: string]: unknown;
}
