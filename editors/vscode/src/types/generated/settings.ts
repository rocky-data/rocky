/* eslint-disable */
/**
 * AUTO-GENERATED — do not edit by hand.
 * Source: schemas/settings.schema.json
 * Run `just codegen` from the monorepo root to regenerate.
 */

/**
 * Concurrency control for remote `[state]` object writes.
 *
 * Guards the cross-process lost update where two writers sharing one `[state]` location race: one reads the state, the other commits, and the first's upload silently overwrites the second (last-writer-wins). See [`StateConfig::concurrency_control`].
 *
 * **Scope:** `docs/adr/ADR-CONCURRENCY.md` governs. It *requires* every write of the shared state object to compare-and-swap, split by writer class (runs refuse on conflict; single-record ledger seams retry). That requirement covers the shared object only: the sibling objects written under their own keys — freeze markers, idempotency records, the doctor read/write probe — are create-once or self-keyed, cannot lose an update, and stay outside compare-and-swap by design rather than as a gap.
 *
 * `cas` satisfies that requirement today for the end-of-run upload, for the `rocky policy` freeze/unfreeze ledger write (whose markers are create-once and must never be replayed, so they stay outside the retry), and — since #1372 — for `rocky gc`. It does not yet for `rocky restore`, which uploads the shared object unconditionally on every remote backend, nor for `rocky apply`'s verify-after custody rows, which take that path only when `verify_after` is configured. So `cas` does not yet make a deployment fully safe against lost updates. Issue #1228 tracks closing that half; `rocky doctor --check state_concurrency` reports the residual exposure in the meantime.
 */
export type ConcurrencyControl = "off" | "cas";
/**
 * What happened when `rocky.toml` was read at server start.
 */
export type ConfigStatus = "loaded" | "absent" | "unreadable";
/**
 * State storage backend variants.
 */
export type StateBackend = "local" | "s3" | "gcs" | "valkey" | "tiered";
/**
 * The spellings `--token-scope` accepts.
 */
export type TokenScopeLabel = "full" | "read-only";
/**
 * Whether `ROCKY_WEBHOOK_SECRET` can actually sign a webhook.
 *
 * Named by operator consequence rather than by error kind — the question this answers is "what happens if I turn the scheduler on?".
 */
export type WebhookSecretStatus = "present" | "absent" | "set_but_unusable";

/**
 * The running server's posture, served by `GET /api/v1/settings`.
 *
 * **An allowlist, not a config dump.** Every field below is projected individually, by hand, in `crate::api::settings_output` — from a [`rocky_server::state::SettingsSnapshot`] of primitives plus two fieldless enum labels taken out of the config file, after which the config is dropped. No `RockyConfig` is serialised or `Debug`-printed anywhere on that path: `AdapterConfig`'s `Debug` prints its `.extra` map, which is unbounded caller-supplied TOML, so a config that merely *passed through* this type would be a disclosure surface.
 *
 * No secret appears — not the Bearer token, not `ROCKY_WEBHOOK_SECRET`. The token is reported as its name and scope; the webhook secret as whether it is usable.
 *
 * To be exact about what enforces that: the **projection function** does, not the type. Rust would happily let a future field carry a secret. What makes it hold is that the projection names every field explicitly, and three tests stand behind it — `settings_reports_exactly_the_allowlisted_fields` fails when a field is *added*, `settings_never_discloses_a_configured_secret` greps this document for three real configured secrets, and `no_safe_route_discloses_a_configured_secret` greps every safe route for the same three.
 *
 * **API-only, deliberately.** There is no `rocky settings` verb, because this document describes *a server that is running* and a one-shot CLI invocation would have to invent one. [`ScheduleStatusOutput`] is the established precedent for a route with no CLI oracle.
 *
 * **Freshness.** Everything except `state_backend` and `concurrency_control` is fixed when the process starts and cannot change while it runs.
 *
 * Those two come from `rocky.toml`, and are read **once, on the first request to this route**, then fixed for the life of the process. Deliberately not at startup: `rocky serve` binds its listener before anything reads that file, and an eager read would let a `rocky.toml` that is a FIFO or sits on a stalled mount stop the server binding at all.
 *
 * So they are a snapshot, not a live view, and the scheduler re-reads that same file every tick — a config edited after the first request to this route is not reflected here, while the scheduler acts on the new one.
 */
export interface SettingsOutput {
  /**
   * Extra `Host` header values the UI guard accepts.
   *
   * This is what is **enforced**, not what was typed: `--allowed-host` is only wired into a guard when `--ui` is on, so this is `[]` whenever `ui` is `false`. Reporting the raw flag list would claim a guard that is not running.
   */
  allowed_hosts: string[];
  /**
   * The CORS allowlist actually installed. Empty means same-origin only.
   *
   * Like `allowed_hosts`, this is what is **enforced**: `build_cors_layer` drops an `--allowed-origin` that is not a valid header value, and an origin it could not install grants nothing. Both this and the layer come from one derivation, so the report cannot drift from the layer.
   */
  allowed_origins: string[];
  /**
   * The host the listener is bound to, verbatim (`127.0.0.1`, `0.0.0.0`, …).
   *
   * The same `String` `ServeConfig` binds — `serve` lends one value to both, so this cannot name a host the server is not actually on.
   */
  bind_host: string;
  /**
   * `[state] concurrency_control`, read at the same moment as `state_backend`. `null` on the same condition.
   */
  concurrency_control?: ConcurrencyControl | null;
  /**
   * What happened when `rocky.toml` was read.
   *
   * Carried so a `null` backend is explainable: a project with no config and a project whose config is broken are different facts, and no other HTTP route distinguishes them today.
   */
  config_status: ConfigStatus;
  /**
   * Whether a resident reconciler is running (`--scheduler`).
   */
  scheduler: boolean;
  /**
   * `[state] backend`, as read on the first request to this route. `null` when there was no readable config — `config_status` says which.
   */
  state_backend?: StateBackend | null;
  /**
   * The configured Bearer token, as its name and scope — never its value.
   *
   * `null` means **no token is configured**, which the server permits only on a loopback bind (`api::serve` refuses to bind a non-loopback host with no auth). That is the most exposure-relevant answer this route gives, so it is a distinguishable `null` rather than a token named "none".
   */
  token?: TokenSettings | null;
  /**
   * Whether the embedded browser UI is served at `/ui/` (`--ui`).
   */
  ui: boolean;
  /**
   * Whether `ROCKY_WEBHOOK_SECRET` can sign a webhook.
   *
   * Reported even when `scheduler` is `false`, which is the point: it tells an operator what will happen when they turn the scheduler on.
   */
  webhook_secret: WebhookSecretStatus;
  [k: string]: unknown;
}
/**
 * The configured Bearer token, described without disclosing it.
 */
export interface TokenSettings {
  /**
   * Always `default`. `rocky serve` holds exactly one token; the name exists so a future multi-token server does not have to change this shape.
   */
  name: string;
  /**
   * What the token may do.
   */
  scope: TokenScopeLabel;
  [k: string]: unknown;
}
