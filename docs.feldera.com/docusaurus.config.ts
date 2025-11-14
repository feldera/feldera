import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";
import type * as PresetOpenapi from "docusaurus-preset-openapi";
import type * as Redocusaurus from "redocusaurus";

const config: Config = {
  title: "Feldera Documentation",
  tagline: "Powerup your Data Lake with Incremental View Maintenance",
  favicon: "img/favicon.ico",

  // Set the production url of your site here
  url: "https://docs.feldera.com",
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: "/",

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: "feldera", // Usually your GitHub org/user name.
  projectName: "feldera", // Usually your repo name.

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  // Even if you don't use internationalization, you can use this field to set
  // useful metadata like html lang. For example, if your site is Chinese, you
  // may want to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "docusaurus-preset-openapi",
      {
        api: {
          path: "../openapi.json",
          routeBasePath: "/api",
        },
        docs: {
          path: "docs",
          sidebarPath: "./docs/sidebars.js",
          routeBasePath: "/",
        },
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies PresetOpenapi.Options,
    ],
    [
      "redocusaurus",
      {
        openapi: {
          // Folder to scan for *.openapi.yaml files
          path: "./openapi/",
          routeBasePath: "/pipelines",
        },
        theme: {
          // Change with your site colors
          primaryColor: "#ae35cc",
          customCss: "./src/css/custom.css",
        },
      },
    ] satisfies Redocusaurus.PresetEntry,
  ],

  plugins: [
    [
      "@docusaurus/plugin-client-redirects",
      {
        redirects: [
          {
            from: "/api/retrieve-authentication-provider-configuration",
            to: "/api/get-auth-config",
          },
          {
            from: "/api/retrieve-general-configuration",
            to: "/api/get-platform-config",
          },
          {
            from: "/api/retrieve-the-list-of-demos",
            to: "/api/list-demos",
          },
          {
            from: "/api/retrieve-current-session-information",
            to: "/api/get-session",
          },
          {
            from: "/api/retrieve-the-list-of-api-keys",
            to: "/api/list-api-keys",
          },
          {
            from: "/api/create-a-new-api-key",
            to: "/api/create-api-key",
          },
          {
            from: "/api/retrieve-an-api-key",
            to: "/api/get-api-key",
          },
          {
            from: "/api/delete-an-api-key",
            to: "/api/delete-api-key",
          },
          {
            from: "/api/get-health",
            to: "/api/check-cluster-health",
          },
          {
            from: "/api/retrieve-the-metrics-of-all-running-pipelines-belonging-to-this-tenant",
            to: "/api/list-all-metrics",
          },
          {
            from: "/api/retrieve-the-list-of-pipelines",
            to: "/api/list-pipelines",
          },
          {
            from: "/api/create-a-new-pipeline",
            to: "/api/create-pipeline",
          },
          {
            from: "/api/retrieve-a-pipeline",
            to: "/api/get-pipeline",
          },
          {
            from: "/api/fully-update-a-pipeline-if-it-already-exists-otherwise-create-a-new-pipeline",
            to: "/api/upsert-pipeline",
          },
          {
            from: "/api/delete-a-pipeline",
            to: "/api/delete-pipeline",
          },
          {
            from: "/api/partially-update-a-pipeline",
            to: "/api/patch-pipeline",
          },
          {
            from: "/api/clears-the-pipeline-storage-asynchronously",
            to: "/api/clear-storage",
          },
          {
            from: "/api/requests-the-pipeline-to-pause-which-it-will-do-asynchronously",
            to: "/api/pause-pipeline",
          },
          {
            from: "/api/requests-the-pipeline-to-resume-which-it-will-do-asynchronously",
            to: "/api/resume-pipeline",
          },
          {
            from: "/api/start-the-pipeline-asynchronously-by-updating-the-desired-status",
            to: "/api/start-pipeline",
          },
          {
            from: "/api/stop-the-pipeline-asynchronously-by-updating-the-desired-state",
            to: "/api/stop-pipeline",
          },
          {
            from: "/api/recompile-a-pipeline-with-the-feldera-runtime-version-included-in-the",
            to: "/api/recompile-pipeline",
          },
          {
            from: "/api/requests-the-pipeline-to-activate-if-it-is-currently-in-standby-mode-which-it-will-do",
            to: "/api/activate-standby-pipeline",
          },
          {
            from: "/api/approves-the-pipeline-to-proceed-with-bootstrapping",
            to: "/api/approve-bootstrap",
          },
          {
            from: "/api/initiates-checkpoint-for-a-running-or-paused-pipeline",
            to: "/api/checkpoint-now",
          },
          {
            from: "/api/syncs-latest-checkpoints-to-the-object-store-configured-in-pipeline-config",
            to: "/api/sync-checkpoints-to-s-3",
          },
          {
            from: "/api/retrieve-status-of-checkpoint-sync-activity-in-a-pipeline",
            to: "/api/get-checkpoint-sync-status",
          },
          {
            from: "/api/retrieve-status-of-checkpoint-activity-in-a-pipeline",
            to: "/api/get-checkpoint-status",
          },
          {
            from: "/api/retrieve-the-circuit-performance-profile-of-a-running-or-paused-pipeline",
            to: "/api/get-performance-profile",
          },
          {
            from: "/api/commit-the-current-transaction",
            to: "/api/commit-transaction",
          },
          {
            from: "/api/check-the-status-of-a-completion-token-returned-by-the-ingress-or-completion-token",
            to: "/api/check-completion-status",
          },
          {
            from: "/api/subscribe-to-a-stream-of-updates-from-a-sql-view-or-table",
            to: "/api/subscribe-to-view",
          },
          {
            from: "/api/retrieve-the-heap-profile-of-a-running-or-paused-pipeline",
            to: "/api/get-heap-profile",
          },
          {
            from: "/api/push-data-to-a-sql-table",
            to: "/api/insert-data",
          },
          {
            from: "/api/retrieve-logs-of-a-pipeline-as-a-stream",
            to: "/api/stream-pipeline-logs",
          },
          {
            from: "/api/retrieve-circuit-metrics-of-a-running-or-paused-pipeline",
            to: "/api/get-pipeline-metrics",
          },
          {
            from: "/api/execute-an-ad-hoc-sql-query-in-a-running-or-paused-pipeline",
            to: "/api/execute-ad-hoc-sql",
          },
          {
            from: "/api/start-a-transaction",
            to: "/api/begin-transaction",
          },
          {
            from: "/api/retrieve-statistics-e-g-performance-counters-of-a-running-or-paused-pipeline",
            to: "/api/get-pipeline-stats",
          },
          {
            from: "/api/generate-a-support-bundle-for-a-pipeline",
            to: "/api/download-support-bundle",
          },
          {
            from: "/api/generate-a-completion-token-for-an-input-connector",
            to: "/api/get-completion-token",
          },
          {
            from: "/api/retrieve-the-status-of-an-input-connector",
            to: "/api/get-input-status",
          },
          {
            from: "/api/start-resume-or-pause-the-input-connector",
            to: "/api/control-input-connector",
          },
          {
            from: "/api/retrieve-time-series-for-statistics-of-a-running-or-paused-pipeline",
            to: "/api/get-time-series-stats",
          },
          {
            from: "/api/stream-time-series-for-statistics-of-a-running-or-paused-pipeline",
            to: "/api/stream-time-series",
          },
          {
            from: "/api/retrieve-the-status-of-an-output-connector",
            to: "/api/get-output-status",
          },
        ],
      },
    ],

    [
      "posthog-docusaurus",
      {
        apiKey: "phc_GKR68l5zo531AD1R3cnE3MCPEBPXTqgYax4q053LVBD",
        enableInDevelopment: true,
      },
    ],
    "docusaurus-plugin-hubspot",
  ],

  themeConfig: {
    //image: 'img/docusaurus-social-card.jpg',
    colorMode: {
      defaultMode: "light",
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },
    hubspot: {
      accountId: 45801078,
      async: true,
      defer: true,
    },
    algolia: {
      appId: "WX24466N8W",
      apiKey: "8cce7b26a5c025f69008e3f843dab4cc",
      indexName: "feldera",
    },
    navbar: {
      logo: {
        alt: "Feldera Logo",
        src: "img/logo-color-dark.svg",
        srcDark: "img/logo-color-light.svg",
        href: "https://www.feldera.com",
      },
      items: [
        {
          label: "Get Started",
          to: "/get-started",
          position: "left",
        },
        {
          label: "SQL Reference",
          to: "/sql",
          position: "left",
        },
        {
          label: "Connectors",
          to: "/connectors",
          position: "left",
        },
        {
          type: "dropdown",
          label: "Community",
          position: "right",
          items: [
            {
              label: "Slack",
              href: "https://join.slack.com/t/felderacommunity/shared_invite/zt-222bq930h-dgsu5IEzAihHg8nQt~dHzA",
            },
            {
              label: "Discord",
              href: "https://discord.com/invite/s6t5n9UzHE",
            },
            {
              label: "GitHub",
              href: "https://github.com/feldera/feldera",
            },
          ],
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            {
              label: "Get Started",
              to: "/get-started",
            },
            {
              label: "Tutorial",
              to: "/tutorials/basics/part1",
            },
            {
              label: "SQL Reference",
              to: "/sql/intro",
            },
          ],
        },
        {
          title: "Community",
          items: [
            {
              label: "Slack",
              href: "https://join.slack.com/t/felderacommunity/shared_invite/zt-222bq930h-dgsu5IEzAihHg8nQt~dHzA",
            },
            {
              label: "Discord",
              href: "https://discord.com/invite/s6t5n9UzHE",
            },
            {
              label: "Bluesky",
              href: "https://bsky.app/profile/feldera.bsky.social",
            },
          ],
        },
        {
          title: "More",
          items: [
            {
              label: "GitHub",
              href: "https://github.com/feldera/feldera",
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Feldera Inc.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
    },
    headTags: [
      // Declare a <link> preconnect tag
      {
        tagName: "link",
        attributes: {
          rel: "preconnect",
          href: "https://feldera.com",
        },
      },
      // Declare some json-ld structured data
      {
        tagName: "script",
        attributes: {
          type: "application/ld+json",
        },
        innerHTML: JSON.stringify({
          "@type": ["Organization", "Brand"],
          name: "Feldera Documentation",
          url: "https://www.feldera.com/",
          logo: "http://www.feldera.com/docs/img/logo.svg",
        }),
      },
    ],
  } satisfies Preset.ThemeConfig,
};

export default config;
