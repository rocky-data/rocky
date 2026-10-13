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
    "/concepts/testing/": "/concepts/data-quality-checks/",
    "/concepts/adapters/": "/guides/adapter-sdk/",
    "/concepts/plan-store-v1-to-v2/": "/advanced/upgrade-notes/",
    "/dagster/schedules/": "/dagster/scheduling/",
    "/dagster/automation/": "/dagster/scheduling/",
    "/dagster/sensors/": "/dagster/scheduling/",
    "/dagster/translator/": "/dagster/assets/",
  },
  integrations: [
    starlight({
      title: "Rocky",
      description: "A SQL transformation engine that checks your pipeline before it runs",
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
          // Concepts is listed by hand, in groups. Add a new concepts page here,
          // or it will not appear in the sidebar.
          label: "Concepts",
          items: [
            {
              label: "Build models",
              items: [
                "concepts/silver-layer",
                "concepts/bronze-layer",
                "concepts/rocky-dsl",
                "concepts/compiler",
                "concepts/dag-dependencies",
                "concepts/schema-patterns",
                "concepts/user-defined-functions",
                "concepts/linters",
                "concepts/data-quality-checks",
              ],
            },
            {
              label: "Run pipelines",
              items: [
                "concepts/execution-flow",
                "concepts/incremental",
                "concepts/time-interval",
                "concepts/content-addressed",
                "concepts/schema-drift",
                "concepts/state-management",
                "concepts/shadow-mode",
                "concepts/hooks",
              ],
            },
            {
              label: "Govern and agents",
              items: [
                "concepts/capability-matrix",
                "concepts/model-governance",
                "concepts/cross-team-contracts",
                "concepts/downstream-consumers",
                "concepts/operating-rocky-with-agents",
                "concepts/mcp-authoring",
                "concepts/ai-intent",
                "concepts/architecture-of-trust",
              ],
            },
            {
              label: "Internals",
              items: [
                "concepts/architecture",
                "concepts/sql-generation",
                "concepts/preview-internals",
              ],
            },
          ],
        },
        {
          label: "Reference",
          items: [{ autogenerate: { directory: "reference" } }],
        },
        {
          label: "Python SDK",
          items: [{ autogenerate: { directory: "python-sdk" } }],
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
