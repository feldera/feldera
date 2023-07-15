// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const math = require('remark-math');
const katex = require('rehype-katex');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "dbsp",
  tagline: "Data In-Motion Processing Made Easy",
  favicon: "img/favicon.ico",

  // Set the production url of your site here
  url: "https://docs.feldera.io",
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: "/",

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: "feldera", // Usually your GitHub org/user name.
  projectName: "dbsp", // Usually your repo name.

  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "docusaurus-preset-openapi",
      /** @type {import('docusaurus-preset-openapi').Options} */
      ({
        docs: {
          sidebarPath: require.resolve("./sidebars.js"),
          /* editUrl: "https://github.com/feldera/dbsp/docs", */
          remarkPlugins: [math],
	  rehypePlugins: [katex],
        },
        api: {
          path: "openapi.json",
          routeBasePath: "/api",
        },
        blog: {
          showReadingTime: false,
          /* editUrl: "https://github.com/feldera/dbsp/docs", */
        },
        theme: {
          customCss: [
            require.resolve("./src/css/custom.css"),
            require.resolve("./src/css/fonts.css"),
          ],
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      algolia: {
        appId: '3SZT2K1T21',
        // Public API key: it is safe to commit it
        apiKey: '959618f671e39b8520016368d09232d2',
        indexName: 'feldera',
        contextualSearch: true,
      },
      colorMode: {
        defaultMode: "dark",
        respectPrefersColorScheme: false,
      },
      navbar: {
        title: "DBSP",
        logo: {
          alt: "DBSP",
          src: "img/logo.svg",
        },
        items: [
          {
            type: 'search',
            position: 'right',
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
                label: "Documentation",
                to: "/docs/what-is-dbsp",
              },
            ],
          },
          {
            title: "Community",
            items: [
              {
                label: "Stack Overflow",
                href: "https://stackoverflow.com/questions/tagged/dbsp",
              },
              {
                label: "Twitter",
                href: "https://twitter.com/felderainc",
              },
            ],
          },
          {
            title: "More",
            items: [
              {
                label: "Blog",
                to: "/blog",
              },
              {
                label: "GitHub",
                href: "https://github.com/feldera",
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Feldera, Inc.`,
      },
    }),

    markdown: {
	mermaid: true,
    },

    themes: ['@docusaurus/theme-mermaid'],

    stylesheets: [
      {
	href: 'https://cdn.jsdelivr.net/npm/katex@0.13.24/dist/katex.min.css',
	type: 'text/css',
	integrity:
	  'sha384-odtC+0UGzzFL/6PNoE8rX/SPcQDXBJ+uRepguP4QkPCm2LBxH3FA3y+fKSiJ+AmM',
	crossorigin: 'anonymous',
      },
    ],
};

module.exports = config;
