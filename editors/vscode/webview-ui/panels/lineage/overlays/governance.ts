import type { GovernanceData } from "../../../../src/webviews/lineage/contract";
import type { LineageOverlay } from "./types";

/**
 * Build a governance overlay: classified models get a lock + count badge,
 * turning red with an "unmasked" count when a classified column has no
 * resolved masking strategy. Matched by name or fqn.
 */
export function makeGovernanceOverlay(data: GovernanceData): LineageOverlay {
  const byModel = new Map(data.models.map((m) => [m.model, m]));
  return {
    id: "governance",
    decorate(node) {
      const gov = byModel.get(node.id) ?? byModel.get(node.data.fqn);
      if (!gov || gov.classifiedColumns === 0) return undefined;
      if (gov.unmaskedColumns > 0) {
        return [
          {
            text: `${gov.unmaskedColumns} unmasked`,
            color: "var(--vscode-charts-red)",
            title: "classified columns without a resolved masking strategy",
          },
        ];
      }
      return [
        {
          text: `🔒 ${gov.classifiedColumns}`,
          color: "var(--vscode-charts-green)",
          title: `${gov.classifiedColumns} classified column(s), all masked`,
        },
      ];
    },
    legend() {
      return [
        { text: "classified + masked", color: "var(--vscode-charts-green)" },
        { text: "unmasked classified", color: "var(--vscode-charts-red)" },
      ];
    },
  };
}
