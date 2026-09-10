/**
 * React Flow paints nothing in jsdom until its canvas reports a size, which
 * is why the estate screen's own test drives the detail pane through the
 * loader instead of clicking. These tests give it that size, so the click
 * and the keyboard run against the real canvas and the real nodes.
 *
 * The shims below are local to this file. Vitest isolates each test file, so
 * the rest of the suite still sees the inert `ResizeObserver` from setup.ts.
 */
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeAll, describe, expect, it, vi } from "vitest";
import type { DagOutput } from "@rocky-types/dag";
import mixedDag from "../test/fixtures/dag-mixed-kinds.json";
import { DagPanel, floorFor, layoutIdentity } from "./DagPanel";
import { NODE_HEIGHT, NODE_WIDTH } from "./layout";

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

beforeAll(() => {
  (globalThis as { ResizeObserver?: unknown }).ResizeObserver = FiringResizeObserver;
  (globalThis as { DOMMatrixReadOnly?: unknown }).DOMMatrixReadOnly = class {
    m22 = 1;
  };
  // jsdom lays nothing out, so every box is 0x0 and React Flow renders no
  // nodes. Report the node width the card asks for, and a canvas big enough
  // to hold the graph.
  Object.defineProperties(HTMLElement.prototype, {
    offsetWidth: { get(this: HTMLElement) { return parseFloat(this.style.width) || 800; } },
    offsetHeight: { get(this: HTMLElement) { return parseFloat(this.style.height) || 480; } },
  });
  (SVGElement.prototype as unknown as { getBBox: () => DOMRect }).getBBox = () =>
    ({ x: 0, y: 0, width: 0, height: 0 }) as DOMRect;
});

const captured = mixedDag as unknown as DagOutput;

/** The rendered node wrapper for a DAG node id. */
function nodeElement(id: string): HTMLElement {
  const el = document.querySelector(`.react-flow__node[data-id="${id}"]`);
  if (!el) throw new Error(`no rendered node for ${id}`);
  return el as HTMLElement;
}

const MODEL = "transformation:customer_orders";
const SOURCE = "source:ecommerce";
const TEST_NODE = "test:revenue_summary::not_null_customer_id";

describe("DagPanel", () => {
  it("opens a transformation node under its bare label, not its prefixed id", () => {
    const onSelect = vi.fn();
    render(<DagPanel dag={captured} onSelect={onSelect} />);
    fireEvent.click(nodeElement(MODEL));
    expect(onSelect).toHaveBeenCalledExactlyOnceWith("customer_orders");
  });

  it.each([
    ["a source node", SOURCE],
    ["a load node", "load:ecommerce"],
    ["a quality node", "quality:nightly_dq"],
    ["a snapshot node", "snapshot:customer_history"],
    ["a seed node", "seed:country_codes"],
    ["a test node", TEST_NODE],
  ])("does not open %s", (_label, id) => {
    const onSelect = vi.fn();
    render(<DagPanel dag={captured} onSelect={onSelect} />);
    fireEvent.click(nodeElement(id));
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("opens a focused transformation node on Enter and on Space", () => {
    const onSelect = vi.fn();
    render(<DagPanel dag={captured} onSelect={onSelect} />);
    const node = nodeElement(MODEL);
    node.focus();
    expect(document.activeElement).toBe(node);

    fireEvent.keyDown(node, { key: "Enter" });
    expect(onSelect).toHaveBeenLastCalledWith("customer_orders");
    fireEvent.keyDown(node, { key: " " });
    expect(onSelect).toHaveBeenCalledTimes(2);
  });

  it("ignores a key that is not Enter or Space", () => {
    const onSelect = vi.fn();
    render(<DagPanel dag={captured} onSelect={onSelect} />);
    fireEvent.keyDown(nodeElement(MODEL), { key: "a" });
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("gives a tab stop and a button role to transformation nodes only", () => {
    render(<DagPanel dag={captured} onSelect={vi.fn()} />);
    const model = nodeElement(MODEL);
    expect(model).toHaveAttribute("tabindex", "0");
    expect(model).toHaveAttribute("role", "button");
    expect(model).not.toHaveAttribute("aria-disabled");

    for (const id of [SOURCE, "load:ecommerce", "quality:nightly_dq", TEST_NODE]) {
      const node = nodeElement(id);
      expect(node).not.toHaveAttribute("tabindex");
      expect(node).toHaveAttribute("aria-disabled", "true");
    }
  });

  it("draws the card at the size the layout declares", () => {
    // Measured in a real browser before this: the wrapper was 184x46 and the
    // card inside it 184x34, so the node carried a 12px invisible clickable
    // band and its handles sat 6px below the card's visual centre. The layout
    // declares this size to React Flow and spaces rows by it, so the card has
    // to actually be it.
    render(<DagPanel dag={captured} onSelect={vi.fn()} />);
    const card = nodeElement(MODEL).firstElementChild as HTMLElement;
    expect(card.style.width).toBe(`${NODE_WIDTH}px`);
    expect(card.style.height).toBe(`${NODE_HEIGHT}px`);
  });

  it("offers a pointer cursor only where there is something to open", () => {
    render(<DagPanel dag={captured} onSelect={vi.fn()} />);
    expect(nodeElement(MODEL).firstElementChild).toHaveClass("cursor-pointer");
    for (const id of [SOURCE, "seed:country_codes", TEST_NODE]) {
      expect(nodeElement(id).firstElementChild).toHaveClass("cursor-default");
    }
  });

  it("does not open a node that has no tab stop, even if a key reaches it", () => {
    const onSelect = vi.fn();
    render(<DagPanel dag={captured} onSelect={onSelect} />);
    fireEvent.keyDown(nodeElement(SOURCE), { key: "Enter" });
    expect(onSelect).not.toHaveBeenCalled();
  });

  it("paints its controls dark when the viewer's system is dark", () => {
    // React Flow's default is a hard-coded `light`, which is why the minimap
    // and the controls used to render light on a dark page. `system` reads
    // the media query instead, so this passes only while it is passed.
    vi.stubGlobal(
      "matchMedia",
      vi.fn((query: string) => ({
        matches: query === "(prefers-color-scheme: dark)",
        media: query,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      })),
    );
    const { container } = render(<DagPanel dag={captured} onSelect={vi.fn()} />);
    expect(container.querySelector(".react-flow")).toHaveClass("dark");
    vi.unstubAllGlobals();
  });

  it("still names every node for a screen reader", async () => {
    render(<DagPanel dag={captured} onSelect={vi.fn()} />);
    const list = await screen.findByRole("list", { name: "Models in the DAG" });
    expect(list.querySelectorAll("li")).toHaveLength(captured.nodes.length);
  });
});

describe("the zoom floor", () => {
  // `minZoom` is the floor for a HAND on the canvas as well as for a fit, so
  // lowering it to fit a phone let a person pinch a node to 9x2 pixels. But a
  // fit below the floor is clamped back up by the next gesture: at 320px an
  // eleven-layer graph already fits at 0.0965, and a 42-layer one at 0.0247,
  // which snapped 5x and threw away most of what the fit had just framed.
  // So the floor follows the fit down, and never up.
  it("keeps its own floor for a graph that fits above it", () => {
    expect(floorFor(0.39)).toBe(0.1);
    expect(floorFor(0.1)).toBe(0.1);
    expect(floorFor(1)).toBe(0.1);
  });

  it("follows a deep graph's fit below it, so the next gesture cannot snap", () => {
    expect(floorFor(0.0965)).toBeCloseTo(0.0965, 6);
    expect(floorFor(0.0247)).toBeCloseTo(0.0247, 6);
  });
});

describe("the refit identity", () => {
  const at = (id: string, x: number, y: number) =>
    ({ id, position: { x, y } }) as Parameters<typeof layoutIdentity>[0][number];

  it("changes when a node moves, even though the ids do not", () => {
    // The case that would otherwise be missed: adding a `depends_on` between
    // two models that already exist re-layers the graph and changes its span,
    // with an identical id set. Keyed on ids alone, nothing would re-fit.
    const before = layoutIdentity([at("a", 0, 0), at("b", 280, 0)]);
    const after = layoutIdentity([at("a", 0, 0), at("b", 560, 0)]);
    expect(after).not.toBe(before);
  });

  it("is stable when nothing about the layout changed", () => {
    const nodes = [at("a", 0, 0), at("b", 280, 0)];
    expect(layoutIdentity(nodes)).toBe(layoutIdentity([...nodes]));
  });

  it("changes when a node is added or removed", () => {
    const two = layoutIdentity([at("a", 0, 0), at("b", 280, 0)]);
    expect(layoutIdentity([at("a", 0, 0)])).not.toBe(two);
    expect(layoutIdentity([at("a", 0, 0), at("b", 280, 0), at("c", 560, 0)])).not.toBe(two);
  });
});
