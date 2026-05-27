/**
 * Allow side-effect CSS imports (Tailwind output, ReactFlow styles) in webview
 * entries. esbuild's `css` loader bundles these; TypeScript only needs to know
 * the import is valid and carries no type.
 */
declare module "*.css";
