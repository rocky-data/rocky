import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ThemeProvider, useTheme } from "./ThemeProvider";

function Probe() {
  return <span>theme:{useTheme()}</span>;
}

describe("ThemeProvider", () => {
  it("renders children and exposes a theme (defaults to dark in jsdom)", () => {
    render(
      <ThemeProvider>
        <Probe />
      </ThemeProvider>,
    );
    // jsdom's <body> carries no `vscode-*` class, so the default is "dark".
    expect(screen.getByText("theme:dark")).toBeInTheDocument();
  });
});
