import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: "https://rocky-data.dev",
  integrations: [
    starlight({
      title: "Rocky",
      description: "Rust SQL transformation engine that replaces dbt",
      logo: {
        dark: "./src/assets/rocky-logo-dark.svg",
        light: "./src/assets/rocky-logo-light.svg",
        alt: "Rocky",
        replacesTitle: true,
      },
      favicon: "/favicon.svg",
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
          items: [{ autogenerate: { directory: "getting-started" } }],
        },
        {
          label: "Guides",
          items: [{ autogenerate: { directory: "guides" } }],
        },
        {
          label: "Concepts",
          items: [{ autogenerate: { directory: "concepts" } }],
        },
        {
          label: "Features",
          items: [{ autogenerate: { directory: "features" } }],
        },
        {
          label: "Reference",
          items: [{ autogenerate: { directory: "reference" } }],
        },
        {
          label: "Dagster Integration",
          items: [{ autogenerate: { directory: "dagster" } }],
        },
        {
          label: "Advanced",
          items: [{ autogenerate: { directory: "advanced" } }],
        },
      ],
    }),
  ],
});
