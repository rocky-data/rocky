import js from "@eslint/js";
import tseslint from "typescript-eslint";

export default tseslint.config(
  { ignores: ["dist/", "node_modules/"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    // The build scripts run under node, not in the page.
    files: ["scripts/**/*.mjs"],
    languageOptions: { globals: { process: "readonly", console: "readonly" } },
  },
  {
    files: ["**/*.{ts,tsx,mjs}"],
    rules: {
      // The shell renders every API value as text. Keep it that way.
      "no-restricted-syntax": [
        "error",
        {
          selector: "JSXAttribute[name.name='dangerouslySetInnerHTML']",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
      ],
    },
  },
);
