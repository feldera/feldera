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
    'what-is-feldera',
    'intro',
    'tour/tour',
    {
      type: 'category',
      label: 'Tutorials',
      link: {
        type: 'doc',
        id: 'tutorials/index'
      },
      items: [
          {
              type: 'category',
              label: 'Feldera: the Basics',
              link: {
                  type: 'doc',
                  id: 'tutorials/basics/index',
              },
              items: [
                  'tutorials/basics/part1',
                  'tutorials/basics/part2',
                  'tutorials/basics/part3'
              ]
          }
      ]
    },
    'demo/demo',
    {
      type: 'category',
      label: 'SQL Reference',
      link: { type: 'doc', id: 'sql/intro' },
      items: [
        'sql/grammar',
        'sql/identifiers',
        'sql/operators',
        'sql/aggregates',
        'sql/casts',
        'sql/types',
        'sql/boolean',
        'sql/comparisons',
        'sql/integer',
        'sql/float',
        'sql/decimal',
        'sql/string',
        'sql/array',
        'sql/datetime'
      ]
    },
    {
      type: 'category',
      label: 'API References',
      items: ['api/rest', 'api/csv', 'api/rust']
    },
    'papers',
    {
      type: 'category',
      label: 'Contributing',
      link: {
	  type: 'doc',
	  id: 'contributors/intro',
      },
      items: ['contributors/compiler', 'contributors/dev-flow']
    }
  ]
}

module.exports = sidebars
