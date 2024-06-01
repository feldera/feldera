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
    'concepts',
    {
      type: 'category',
      label: 'Get Started',
      link: {
        type: 'doc',
        id: 'get-started'
      },
      items: [
	'docker',
	'sandbox',
      ]
    },
    {
      type: 'category',
      label: 'Deploy',
      link: { type: 'doc', id: 'deploy' },
      items: [
        'docker',
        {
          type: 'category',
          label: 'Bring Your Own Cloud',
          link: { type: 'doc', id: 'cloud/index' },
          items: [
            'cloud/assets',
            'cloud/deployment',
            'cloud/secret-management'
          ]
        },
      ]
    },
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
              label: 'Interactive',
              link: {
                  type: 'doc',
                  id: 'tutorials/basics/index',
              },
              items: [
                  'tutorials/basics/part1',
                  'tutorials/basics/part2',
                  'tutorials/basics/part3'
              ]
          },
          'tutorials/rest_api/index'
      ]
    },
    {
      type: 'category',
      label: 'Use Cases',
      items: [
          {
              type: 'doc',
              id: 'use_cases/fraud_detection/fraud_detection',
              label: 'Real-time Fraud Detection',
          },
          {
              type: 'doc',
              id: 'tour/tour',
              label: 'Security Operations',
          }
      ]
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
            {
              type: 'link',
              label: "Python SDK",
              href: "pathname:///python/index.html",
            },
            'api/rest', 
	    {
	      type: 'category',
	      label: 'Connectors',
	      link: {
		type: 'doc',
		id: 'connectors/index'
	      },
	      items: [
              {
                  type: 'category',
                  label: 'Input',
                  link: {
                      type: 'doc',
                      id: 'connectors/sources/index',
                  },
                  items: [
                      {
                          type: 'doc',
                          id: 'connectors/sources/http',
                          label: 'HTTP'
                      },
                      {
                          type: 'doc',
                          id: 'connectors/sources/http-get',
                          label: 'HTTP GET (URL)'
                      },
                      {
                          type: 'doc',
                          id: 'connectors/sources/delta',
                          label: 'Delta Lake'
                      },                      
                      {
                          type: 'doc',
                          id: 'connectors/sources/kafka',
                          label: 'Kafka'
                      },
                      {
                          type: 'doc',
                          id: 'connectors/sources/debezium-mysql',
                          label: 'Debezium-MySQL'
                      }
                  ]
              },
              {
                  type: 'category',
                  label: 'Output',
                  link: {
                      type: 'doc',
                      id: 'connectors/sinks/index',
                  },
                  items: [
                      {
                          type: 'doc',
                          id: 'connectors/sinks/http',
                          label: 'HTTP'
                      },
                      {
                          type: 'doc',
                          id: 'connectors/sinks/delta',
                          label: 'Delta Lake'
                      },
                      {
                          type: 'doc',
                          id: 'connectors/sinks/kafka',
                          label: 'Kafka'
                      },
                      {
                          type: 'doc',
                          id: 'connectors/sinks/snowflake',
                          label: 'Snowflake (experimental)'
                      }
                  ]
              }
	      ]
	    },
            {
              type: 'category',
	      label: 'Formats',
	      items: [
		'api/json', 
		'api/parquet', 
                'api/csv',
	      ],
	    },
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
                'sql/binary',
                'sql/array',
                'sql/datetime',
                'sql/streaming',
                'sql/udf'
              ]
            },
            'api/rust']
    },
    {
      type: 'category',
      label: 'Learn',
      items: ['papers', 'videos']
    },
    {
      type: 'category',
      label: 'Contributing',
      link: {
        type: 'doc',
        id: 'contributors/intro',
      },
      items: ['contributors/compiler', 'contributors/dev-flow', 'contributors/ui-testing']
    }
  ]
}

module.exports = sidebars
