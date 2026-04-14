import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: "https://rocky-data.github.io",
  base: "/rocky",
  integrations: [
    starlight({
      title: "Rocky",
      description: "Rust SQL transformation engine that replaces dbt",
      logo: {
        dark: "./src/assets/rocky-logo-dark.svg",
        light: "./src/assets/rocky-logo-light.svg",
        alt: "Rocky",
      },
      favicon: "/favicon.png",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/rocky-data/rocky",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/rocky-data/rocky/edit/main/docs/",
      },
      sidebar: [
        {
          label: "Getting Started",
          autogenerate: { directory: "getting-started" },
        },
        {
          label: "Guides",
          autogenerate: { directory: "guides" },
        },
        {
          label: "Concepts",
          autogenerate: { directory: "concepts" },
        },
        {
          label: "Features",
          autogenerate: { directory: "features" },
        },
        {
          label: "Reference",
          autogenerate: { directory: "reference" },
        },
        {
          label: "Dagster Integration",
          autogenerate: { directory: "dagster" },
        },
        {
          label: "Advanced",
          autogenerate: { directory: "advanced" },
        },
      ],
    }),
  ],
});
