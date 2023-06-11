/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    "intro",
    "what-is-dbsp",
    {
      type: "category",
      label: "Demos",
      link: {
        type: "generated-index",
        description: "A walkthrough of the pre-existing DBSP demos.",
      },
      items: [
        "demos/simple-select",
        "demos/time-series-enrich",
        "demos/fraud-detection",
        "demos/secops",
      ],
    },
    {
      type: "category",
      label: "Guides",
      items: ["guides/your-first-program"],
    },
    {
      type: "category",
      label: "SQL Reference",
      link: { type: "doc", id: "sql/intro" },
      items: [
        "sql/structure",
        "sql/types",
        "sql/boolean",
        "sql/comparisons",
        "sql/integer",
        "sql/float",
        "sql/decimal",
        "sql/fp",
        "sql/string",
      ],
    },
    {
      type: "category",
      label: "API Reference",
      items: ["api/rest", "api/dbsp", "api/python"],
    },
    "contribute-to-dbsp",
  ],
};

module.exports = sidebars;
