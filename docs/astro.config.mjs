import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: "https://rocky-data.dev",
  redirects: {
    "/features/schema-drift/": "/concepts/schema-drift/",
    "/features/data-quality-checks/": "/concepts/data-quality-checks/",
    "/features/sql-generation/": "/concepts/sql-generation/",
    "/features/linters/": "/concepts/linters/",
    "/features/time-interval/": "/concepts/time-interval/",
    "/features/authentication/": "/reference/authentication/",
    "/features/permissions/": "/reference/permissions/",
    "/features/fivetran-state-cache/": "/reference/fivetran-state-cache/",
    "/features/comparison/": "/getting-started/comparison/",
    "/features/benchmarks/": "/getting-started/benchmarks/",
    "/features/all-features/": "/getting-started/introduction/",
  },
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
