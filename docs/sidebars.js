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
    "what-is-dbsp",
    "intro",
    "tour/tour",
    {
      type: "category",
      label: "Demos",
      link: {
        type: "doc",
        id: 'demos/index',
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
        items: [
            "guides/overview",
            "guides/rust",
            "guides/sql"
        ],
    },
    {
      type: "category",
      label: "SQL Reference",
      link: { type: "doc", id: "sql/intro" },
      items: [
          "sql/grammar",
          "sql/identifiers",
          "sql/operators",
          "sql/aggregates",
          "sql/casts",
          "sql/types",
          "sql/boolean",
          "sql/comparisons",
          "sql/integer",
          "sql/float",
          "sql/decimal",
          "sql/string",
          "sql/array",
          "sql/datetime"
      ],
    },
    {
      type: "category",
      label: "API Reference",
      items: ["api/rest", "api/dbsp", "api/python"],
    },
    "papers",
    "contribute-to-dbsp",
  ],
};

module.exports = sidebars;
