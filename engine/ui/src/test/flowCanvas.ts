/**
 * Give React Flow a canvas in jsdom, so a test can click and focus real nodes.
 *
 * React Flow paints nothing until its canvas reports a size, and jsdom lays
 * nothing out. Call this from `beforeAll` in a test file that needs rendered
 * nodes. Vitest isolates each test file, so the rest of the suite still sees
 * the inert `ResizeObserver` from `setup.ts`.
 */

const CANVAS = { width: 800, height: 480 };

/** A ResizeObserver that reports once, so React Flow measures its canvas. */
class FiringResizeObserver {
  constructor(private readonly callback: ResizeObserverCallback) {}
  observe(target: Element): void {
    const entry = {
      target,
      contentRect: { ...CANVAS, x: 0, y: 0, top: 0, left: 0, bottom: 480, right: 800 },
    } as ResizeObserverEntry;
    this.callback([entry], this as unknown as ResizeObserver);
  }
  unobserve(): void {}
  disconnect(): void {}
}

export function installFlowCanvas(): void {
  (globalThis as { ResizeObserver?: unknown }).ResizeObserver = FiringResizeObserver;
  (globalThis as { DOMMatrixReadOnly?: unknown }).DOMMatrixReadOnly = class {
    m22 = 1;
  };
  // Every box is 0x0 in jsdom, so React Flow renders no nodes. Report the
  // node width the card asks for, and a canvas big enough to hold the graph.
  Object.defineProperties(HTMLElement.prototype, {
    offsetWidth: { get(this: HTMLElement) { return parseFloat(this.style.width) || 800; } },
    offsetHeight: { get(this: HTMLElement) { return parseFloat(this.style.height) || 480; } },
  });
  (SVGElement.prototype as unknown as { getBBox: () => DOMRect }).getBBox = () =>
    ({ x: 0, y: 0, width: 0, height: 0 }) as DOMRect;
}

/** The rendered node wrapper for a DAG node id. */
export function nodeElement(id: string): HTMLElement {
  const el = document.querySelector(`.react-flow__node[data-id="${id}"]`);
  if (!el) throw new Error(`no rendered node for ${id}`);
  return el as HTMLElement;
}
