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
      "posthog-docusaurus",
      {
        apiKey: "phc_GKR68l5zo531AD1R3cnE3MCPEBPXTqgYax4q053LVBD",
        enableInDevelopment: true,
      },
    ],
    'docusaurus-plugin-hubspot'
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
