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
 *   transformation  the model name        200, if the server compiled it
 *   source          "{pipeline} (source)" 404
 *   load            "{pipeline} (load)"   404
 *   quality         the pipeline name     404
 *   snapshot        the pipeline name     404
 *   replication     the pipeline name     404   (legacy; see below)
 *   seed            the seed name         404
 *   test            the test label        404
 *
 * A transformation node is not enough on its own. The DAG reads every
 * transformation pipeline's own models directory, and the server compiles
 * one directory, so the graph can name a model the route has never compiled
 * (#2011). Such a node is `not-compiled`. The compiled set is
 * `GET /api/v1/models`, which reads the same compile the detail route reads.
 *
 * The two reads are not one snapshot. `/dag` reads the files on disk at
 * request time; `/models` reads the last compile, which `serve --watch`
 * replaces a moment after a file changes. So a model can be `not-compiled`
 * only because the compile has not caught up yet. The estate screen reads
 * the list again while any node is `not-compiled`, so that state corrects
 * itself instead of lasting until the next Refresh.
 *
 * When that set is not known (still loading, refused, unreachable), a
 * transformation node stays servable. Not knowing is not evidence that a
 * model is absent: gating on nothing would disable every model on one
 * failed read. The pane then shows whatever the detail route answers.
 *
 * A kind this table does not name is reported as `unknown-kind` rather than
 * folded into "not servable". The engine's `NodeKind` is a Rust enum but
 * reaches this package as a bare `string` (`dag.ts`), so a kind added in
 * Rust cannot break this build. Naming the third state is what lets a test
 * fail when one appears. Callers treat it as unservable.
 */

import type { ModelListOutput } from "@rocky-types/model_list";

/** The model names the server compiled, or `"unknown"` when that is not known. */
export type CompiledModels = ReadonlySet<string> | "unknown";

/** What the model route can do with one DAG node. */
export type NodeRoute =
  | { readonly state: "servable"; readonly model: string }
  | { readonly state: "not-compiled"; readonly model: string }
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
 * The node's route, from the two fields every DAG node carries and the
 * compiled set. `compiled` is required, so no caller can forget the set and
 * silently offer a model the route cannot serve. The name must match
 * exactly: the route looks the model up by its exact name, with no case
 * folding.
 */
export function nodeRoute(
  node: { readonly kind: string; readonly label: string },
  compiled: CompiledModels,
): NodeRoute {
  const entry = KIND_TABLE.get(node.kind);
  if (entry === undefined) return { state: "unknown-kind", kind: node.kind };
  if (entry === "not-servable") return { state: "not-servable" };
  if (compiled !== "unknown" && !compiled.has(node.label)) {
    return { state: "not-compiled", model: node.label };
  }
  return { state: "servable", model: node.label };
}

/** Whether any of these nodes is a model the server did not compile. */
export function anyNotCompiled(
  nodes: readonly { readonly kind: string; readonly label: string }[],
  compiled: CompiledModels,
): boolean {
  return nodes.some((node) => nodeRoute(node, compiled).state === "not-compiled");
}

/**
 * Whether the list carries every model it says it does. `count` repeats
 * `models.len()` so a consumer can tell a cut list from a whole one.
 */
export function isWholeList(list: ModelListOutput): boolean {
  return list.count === list.models.length;
}

/**
 * The compiled set a model list proves. A cut list proves nothing: it would
 * mark a real model as not compiled, so it is `"unknown"`. An empty whole
 * list is a known empty set, not an unknown one.
 */
export function compiledModels(list: ModelListOutput): CompiledModels {
  if (!isWholeList(list)) return "unknown";
  return new Set(list.models.map((model) => model.name));
}
