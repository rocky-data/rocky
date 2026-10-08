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
      // The shell renders every API value as text. Keep it that way. This is
      // the `react/no-danger` rule, written with the plugins installed here,
      // and it also covers the non-JSX ways to inject markup.
      "no-restricted-syntax": [
        "error",
        {
          selector: "JSXAttribute[name.name='dangerouslySetInnerHTML']",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
        {
          selector: "Property[key.name='dangerouslySetInnerHTML']",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
        {
          selector:
            "AssignmentExpression[left.type='MemberExpression'][left.property.name=/^(innerHTML|outerHTML)$/]",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
        {
          selector:
            "AssignmentExpression[left.type='MemberExpression'][left.property.value=/^(innerHTML|outerHTML|srcdoc)$/]",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
        {
          selector: "AssignmentExpression[left.type='MemberExpression'][left.property.name='srcdoc']",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
        {
          selector: "CallExpression[callee.property.name='insertAdjacentHTML']",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
        {
          selector:
            "CallExpression[callee.object.name='document'][callee.property.name=/^(write|writeln)$/]",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
        {
          selector: "CallExpression[callee.property.name='createContextualFragment']",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
        {
          selector: "JSXAttribute[name.name=/^src[dD]oc$/]",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
        {
          selector:
            "CallExpression[callee.property.name='setAttribute'][arguments.0.value=/^srcdoc$/i]",
          message: "render API values as text; the UI carries no HTML from the engine",
        },
      ],
    },
  },
);
