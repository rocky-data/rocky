import type { ModelNodeData } from "../layout";

/** A small chip rendered on a node by an overlay. */
export interface Badge {
  text: string;
  /** CSS background color; defaults to the neutral badge color. */
  color?: string;
  title?: string;
}

/** A node as seen by an overlay's decorator. */
export interface NodeView {
  id: string;
  data: ModelNodeData;
}

/** A legend entry describing what a color means. */
export interface LegendItem {
  text: string;
  color: string;
}

/**
 * A composable graph decoration. Each Tier-2/3 feature is one overlay: it maps
 * a node to zero or more badges, so the canvas gains a feature without the
 * canvas (or other overlays) knowing about it.
 */
export interface LineageOverlay {
  id: string;
  decorate(node: NodeView): Badge[] | undefined;
  legend?(): LegendItem[];
}
