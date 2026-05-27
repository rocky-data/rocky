import tseslint from "typescript-eslint";

const noUnusedVars = {
  "@typescript-eslint/no-unused-vars": [
    "error",
    { argsIgnorePattern: "^_", varsIgnorePattern: "^_" },
  ],
};

export default tseslint.config(
  { ignores: ["out/", "dist/", "node_modules/", "src/types/generated/"] },
  ...tseslint.configs.recommended,
  {
    files: ["src/**/*.ts"],
    rules: noUnusedVars,
  },
  {
    files: ["webview-ui/**/*.{ts,tsx}"],
    rules: noUnusedVars,
  },
);
