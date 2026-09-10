/**
 * Which DAG nodes the model detail route can serve, and under what name.
 *
 * `GET /api/v1/models/{name}` searches the compiled model set. Only a
 * transformation node names a compiled model, and it names it in `label`.
 * It does **not** name it in `id`: every id carries a `kind:` prefix that
 * the route never strips, so `/models/transformation:customer_orders` is a
 * 404 and `/models/customer_orders` is a 200.
 *
 * Every other kind labels something the route cannot find — a pipeline, a
 * seed, a declarative test — and two of them label a phrase with a space
 * in it (`ecommerce (source)`), which is not an identifier at all.
 *
 *   kind            label is              /models/{label}
 *   ─────────────────────────────────────────────────────
 *   transformation  the model name        200
 *   source          "{pipeline} (source)" 404
 *   load            "{pipeline} (load)"   404
 *   quality         the pipeline name     404
 *   snapshot        the pipeline name     404
 *   replication     the pipeline name     404   (legacy; see below)
 *   seed            the seed name         404
 *   test            the test label        404
 *
 * A kind this table does not name is reported as `unknown-kind` rather than
 * folded into "not servable". The engine's `NodeKind` is a Rust enum but
 * reaches this package as a bare `string` (`dag.ts`), so a kind added in
 * Rust cannot break this build. Naming the third state is what lets a test
 * fail when one appears. Callers treat it as unservable.
 */

/** What the model route can do with one DAG node. */
export type NodeRoute =
  | { readonly state: "servable"; readonly model: string }
  | { readonly state: "not-servable" }
  | { readonly state: "unknown-kind"; readonly kind: string };

/**
 * One entry per variant of `NodeKind` in `unified_dag.rs`, in the order it
 * is declared there. `replication` is legacy: the parser expands a
 * replication pipeline into a `source` + `load` pair, and the variant
 * survives only so a stored DAG still deserializes. It is listed because a
 * stored DAG can still carry it.
 */
const KIND_TABLE = new Map<string, "servable" | "not-servable">([
  ["source", "not-servable"],
  ["replication", "not-servable"],
  ["transformation", "servable"],
  ["quality", "not-servable"],
  ["snapshot", "not-servable"],
  ["load", "not-servable"],
  ["seed", "not-servable"],
  ["test", "not-servable"],
]);

/** Every kind this module classifies. Read by the tests, not by the UI. */
export const CLASSIFIED_KINDS: readonly string[] = [...KIND_TABLE.keys()];

/**
 * The node's route, from the two fields every DAG node carries. Takes the
 * shape rather than `DagNodeOutput` so the flow node's `data` — which keeps
 * the same two fields — can ask the same question without a second table.
 */
export function nodeRoute(node: { readonly kind: string; readonly label: string }): NodeRoute {
  const entry = KIND_TABLE.get(node.kind);
  if (entry === undefined) return { state: "unknown-kind", kind: node.kind };
  if (entry === "not-servable") return { state: "not-servable" };
  return { state: "servable", model: node.label };
}
