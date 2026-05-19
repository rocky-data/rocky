/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/rocky_fivetran_state.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Envelope schema version. A newtype enum (rather than a free-form string) so adding a new variant is a compile-time event and so downstream deserializers can refuse unknown versions at parse time instead of accepting a string they can't actually handle.
 */
export type EnvelopeVersion = "1.0";

/**
 * Canonical Fivetran state envelope written by `rocky discover --emit-fivetran-state-to <PATH>`.
 *
 * The envelope is the single contract between Rocky and downstream consumers that want a snapshot of the Fivetran view of a destination — connectors, their sync states, and the per-connector schema config — without re-fetching from Fivetran themselves.
 *
 * ## Construction invariants
 *
 * - [`Self::connectors`] is sorted ascending by `connector.id` at construction time via [`Self::from_parts`]. - [`Self::schemas`] is a [`BTreeMap`] so iteration order is the ascending lexicographic order of `connector_id` keys.
 *
 * These guarantees make the serialized bytes byte-stable across runs (modulo [`Self::fetched_at`]), which is what makes [`envelope_hash`] a useful sentinel for the idempotent emit path.
 */
export interface FivetranStateEnvelope {
  /**
   * Connector summaries sorted ascending by `connector.id` — see the construction-invariant note on the type doc.
   */
  connectors: FivetranConnectorSummary[];
  destination: FivetranDestination;
  fetched_at: string;
  /**
   * Schema config keyed by connector id. [`BTreeMap`] for stable iteration order.
   */
  schemas: {
    [k: string]: FivetranSchemaConfig;
  };
  version: EnvelopeVersion;
  [k: string]: unknown;
}
/**
 * Per-connector summary projected into the envelope.
 *
 * Strict superset of what `dagster_fivetran.FivetranWorkspaceData` consumes — the Python side can derive that shape from this one losslessly. A round-trip parity test against the dagster-fivetran public type lives on the Python side and is tracked as a follow-up.
 */
export interface FivetranConnectorSummary {
  /**
   * Last failed sync timestamp, if any.
   */
  failed_at?: string | null;
  /**
   * Fivetran group ID owning this connector.
   */
  group_id?: string | null;
  id: string;
  /**
   * Human-friendly connector name. Fivetran returns this on the `groups/{id}/connectors` payload but the adapter currently captures only the `schema` field; the envelope surfaces both so downstream UIs can render the friendly name without re-querying. Falls back to `schema` when the upstream payload omits `name`.
   */
  name: string;
  /**
   * Whether the connector is paused.
   */
  paused: boolean;
  /**
   * Destination schema this connector writes into.
   */
  schema: string;
  /**
   * Fivetran service tag (e.g. `"shopify"`).
   */
  service: string;
  /**
   * Setup + sync state at fetch time.
   */
  status: FivetranConnectorStatus;
  /**
   * Last successful sync timestamp, if any.
   */
  succeeded_at?: string | null;
  [k: string]: unknown;
}
/**
 * Sync-status block on a connector. Two fields today; left as a struct rather than flattened so future Fivetran additions land without breaking the envelope shape.
 */
export interface FivetranConnectorStatus {
  setup_state: string;
  sync_state: string;
  [k: string]: unknown;
}
/**
 * Fivetran destination metadata projected into the envelope.
 *
 * Fields use the same names Fivetran's `GET /v1/destinations/{id}` endpoint returns — Rocky's adapter takes the wire shape verbatim so downstream consumers can move between the envelope and the upstream API without rename pain.
 */
export interface FivetranDestination {
  /**
   * Fivetran destination identifier (e.g. `"dest_xyz"`). Globally unique within an account.
   */
  id: string;
  /**
   * Region the destination is hosted in (e.g. `"us-east-1"`). Optional — older destination records may omit it.
   */
  region?: string | null;
  /**
   * Fivetran service tag for the destination warehouse (e.g. `"snowflake"`, `"big_query"`). Optional.
   */
  service?: string | null;
  /**
   * Setup status of the destination, when present.
   */
  setup_status?: string | null;
  /**
   * Destination time zone (e.g. `"UTC"`). Optional for the same reason as `region`.
   */
  time_zone?: string | null;
  [k: string]: unknown;
}
/**
 * Schema config payload from `GET /v1/connectors/{id}/schemas`, projected into envelope shape with [`BTreeMap`]s for stable iteration order.
 */
export interface FivetranSchemaConfig {
  /**
   * Map of logical schema key → schema entry.
   */
  schemas?: {
    [k: string]: FivetranSchemaEntry;
  };
  [k: string]: unknown;
}
/**
 * One schema entry inside the per-connector schema config.
 */
export interface FivetranSchemaEntry {
  /**
   * Whether this destination schema is enabled.
   */
  enabled?: boolean;
  /**
   * Destination schema name when Fivetran has renamed it.
   */
  name_in_destination?: string | null;
  /**
   * Tables in this schema. [`BTreeMap`] for stable iteration order.
   */
  tables?: {
    [k: string]: FivetranTableConfig;
  };
  [k: string]: unknown;
}
/**
 * One table entry inside the per-connector schema config.
 */
export interface FivetranTableConfig {
  /**
   * Per-column config. [`BTreeMap`] for stable iteration order so the envelope hash doesn't drift on HashMap shuffles.
   */
  columns?: {
    [k: string]: FivetranColumnConfig;
  };
  /**
   * Whether this table is enabled for sync.
   */
  enabled?: boolean;
  /**
   * Destination table name when Fivetran renames it (manual override or the `do_not_alter_` auto-prefix after a breaking schema change). The destination name is what shows up in `information_schema.tables`.
   */
  name_in_destination?: string | null;
  /**
   * Sync mode (e.g. `"SOFT_DELETE"`), when set.
   */
  sync_mode?: string | null;
  [k: string]: unknown;
}
/**
 * One column entry inside the per-table schema config.
 */
export interface FivetranColumnConfig {
  /**
   * Whether this column is enabled for sync.
   */
  enabled?: boolean;
  /**
   * Whether Fivetran is applying its column-hash transform on this column (PII masking flag).
   */
  hashed?: boolean;
  [k: string]: unknown;
}
