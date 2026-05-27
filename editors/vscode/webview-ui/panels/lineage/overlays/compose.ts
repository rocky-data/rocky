import type { Badge, LineageOverlay, NodeView } from "./types";

/** Run every active overlay against a node and concatenate the badges, in order. */
export function composeBadges(
  node: NodeView,
  overlays: LineageOverlay[],
): Badge[] {
  const badges: Badge[] = [];
  for (const overlay of overlays) {
    const decoration = overlay.decorate(node);
    if (decoration) badges.push(...decoration);
  }
  return badges;
}
