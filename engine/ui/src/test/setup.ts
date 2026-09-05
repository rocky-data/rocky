import "@testing-library/jest-dom/vitest";

// jsdom has no ResizeObserver; React Flow measures its canvas with one.
if (typeof globalThis.ResizeObserver === "undefined") {
  class ResizeObserverShim {
    observe(): void {}
    unobserve(): void {}
    disconnect(): void {}
  }
  (globalThis as { ResizeObserver?: unknown }).ResizeObserver = ResizeObserverShim;
}
