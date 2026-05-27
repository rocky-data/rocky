/** Theme-aware node accents drawn from VS Code's chart palette. */
function chart(name: string): string {
  return `var(--vscode-charts-${name})`;
}

/** Accent color for a resource kind (the "color by type" mode). */
export function kindColor(kind: string): string {
  switch (kind) {
    case "Source":
      return chart("green");
    case "Model":
      return chart("blue");
    case "View":
      return chart("purple");
    case "MaterializedView":
      return chart("orange");
    default:
      return chart("foreground");
  }
}

/** Accent color for a materialization strategy (the "color by materialization" mode). */
export function materializationColor(materialization: string | null): string {
  switch (materialization) {
    case "full_refresh":
      return chart("blue");
    case "incremental":
    case "microbatch":
      return chart("green");
    case "merge":
    case "delete_insert":
      return chart("purple");
    case "time_interval":
      return chart("orange");
    case "view":
    case "ephemeral":
      return chart("foreground");
    case "materialized_view":
    case "dynamic_table":
    case "content_addressed":
      return chart("yellow");
    default:
      return chart("foreground");
  }
}

/** Short glyph shown on a node, by resource kind. */
export function kindGlyph(kind: string): string {
  switch (kind) {
    case "Source":
      return "SRC";
    case "Model":
      return "MDL";
    case "View":
      return "VW";
    case "MaterializedView":
      return "MV";
    default:
      return "•";
  }
}
