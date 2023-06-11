// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

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
          editUrl: "https://github.com/feldera/dbsp/docs",
        },
        api: {
          path: "openapi.json",
          routeBasePath: "/api",
        },
        blog: {
          showReadingTime: true,
          editUrl: "https://github.com/feldera/dbsp/docs",
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
      colorMode: {
        defaultMode: "dark",
        respectPrefersColorScheme: false,
      },
      navbar: {
        style: "dark",
        title: "DBSP",
        logo: {
          alt: "DBSP",
          src: "img/logo.svg",
        },
        items: [
          {
            type: "docSidebar",
            sidebarId: "docsSidebar",
            position: "right",
            label: "Get Started",
          },
          {
            position: "right",
            label: "REST API",
            to: "/api",
          },
          { to: "/blog", label: "Blog", position: "right" },
          {
            href: "https://github.com/feldera/dbsp",
            label: "GitHub",
            position: "right",
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
                label: "Tutorial",
                to: "/docs/intro",
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
};

module.exports = config;
