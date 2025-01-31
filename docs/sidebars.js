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
        {
            type: 'category',
            label: 'Install Feldera',
            link: {
                type: 'doc',
                id: 'get-started'
            },
            items: [
                'docker',
                'sandbox',
                {
                    type: 'category',
                    label: 'Feldera Enterprise',
                    link: {
                        type: 'doc',
                        id: 'enterprise/index'
                    },
                    items: [
                        'enterprise/quickstart',
                        'enterprise/helm-guide',
                        {
                            type: 'category',
                            label: 'Kubernetes guides',
                            link: {
                                type: 'doc',
                                id: 'enterprise/kubernetes-guides/index',
                            },
                            items: [
                                'enterprise/kubernetes-guides/k3d',
                                {
                                    type: 'category',
                                    label: 'EKS',
                                    link: {
                                        type: 'doc',
                                        id: 'enterprise/kubernetes-guides/eks/index',
                                    },
                                    items: [
                                        'enterprise/kubernetes-guides/eks/cluster',
                                        'enterprise/kubernetes-guides/eks/ingress'
                                    ]
                                },
                                'enterprise/kubernetes-guides/secret-management'
                            ]
                        }
                    ]
                },
            ]
        },
        {
            type: 'category',
            label: 'Guides',
            link: {
                type: 'doc',
                id: 'tutorials/index',
            },
            items: [
                {
                    type: 'category',
                    label: 'Feldera Basics',
                    link: {
                        type: 'doc',
                        id: 'tutorials/basics/index',
                    },
                    items: [
                        'tutorials/basics/part1',
                        'tutorials/basics/part2',
                        'tutorials/basics/part3',
                        'tutorials/basics/part4'
                    ]
                },
                {
                    type: 'doc',
                    id: 'use_cases/fraud_detection/fraud_detection',
                    label: 'Real-time Fraud Detection',
                },
                {
                    type: 'category',
                    label: 'Real-time Web Applications',
                    items: [
                        'use_cases/real_time_apps/part1',
                        'use_cases/real_time_apps/part2',
                        'use_cases/real_time_apps/part3',
                    ]
                },
                'tutorials/time-series',
                {
                    type: 'category',
                    label: 'Fine-Grained Authorization',
                    link: {
                        type: 'doc',
                        id: 'use_cases/fine_grained_authorization/intro'
                    },
                    items: [
                        'use_cases/fine_grained_authorization/static',
                        'use_cases/fine_grained_authorization/dynamic'
                    ]
                },
                {
                    type: 'category',
                    label: 'OpenTelemetry Analysis',
                    link: {
                        type: 'doc',
                        id: 'use_cases/otel/intro'
                    },
                    items: [
                        'use_cases/otel/representing_otel_data',
                        'use_cases/otel/preprocessing',
                        'use_cases/otel/insights',
                        'use_cases/otel/grafana'
                    ]
                },
                'tutorials/rest_api/index',
                'tutorials/monitoring/index'
            ]
        },
        {
            type: 'category',
            label: 'Feldera Interface',
            collapsed: false,
            link: {
                type: 'doc',
                id: 'interact/index'
            },
            items: [
                'reference/web-console',
                'reference/cli',
                {
                    type: 'link',
                    label: "Python SDK",
                    href: "pathname:///python/",
                },
                {
                    type: 'link',
                    label: "REST API",
                    href: "pathname:///api", // ../../../api
                }
            ]
        },
        {
            type: 'category',
            label: 'Pipelines',
            collapsed: false,
            link: {
                type: 'doc',
                id: 'writing/index'
            },
            items: [
                {
                    type: 'category',
                    label: 'Feldera SQL',
                    link: {
                        type: 'doc',
                        id: 'sql/index'
                    },
                    items: [
                        'sql/grammar',
                        'sql/identifiers',
                        'sql/types',
                        'sql/casts',
                        'sql/operators',
                        'sql/aggregates',
                        'sql/boolean',
                        'sql/comparisons',
                        'sql/integer',
                        'sql/float',
                        'sql/decimal',
                        'sql/string',
                        'sql/uuid',
                        'sql/binary',
                        'sql/array',
                        'sql/map',
                        'sql/datetime',
                        'sql/json',
                        'sql/materialized',
                        'sql/recursion',
                        'sql/ad-hoc',
                        'sql/streaming',
                        'sql/system',
                        'sql/table',
                        'sql/udf',
                        'sql/function-index'
                    ]
                },
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
                                    id: 'connectors/sources/iceberg',
                                    label: 'Apache Iceberg'
                                },
                                {
                                    type: 'doc',
                                    id: 'connectors/sources/kafka',
                                    label: 'Kafka'
                                },
                                {
                                    type: 'doc',
                                    id: 'connectors/sources/pubsub',
                                    label: 'Google Pub/Sub'
                                },
                                {
                                    type: 'doc',
                                    id: 'connectors/sources/debezium',
                                    label: 'Debezium'
                                },
				{
				    type: 'doc',
				    id: 'connectors/sources/postgresql',
				    label: 'PostgreSQL'
				},
                                {
                                    type: 'doc',
                                    id: 'connectors/sources/s3',
                                    label: 'AWS S3'
                                },
                                {
                                    type: 'doc',
                                    id: 'connectors/sources/datagen',
                                    label: 'Data Generator'
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
                                    id: 'connectors/sinks/confluent-jdbc',
                                    label: 'Confluent JDBC Connector'
                                },
                                {
                                    type: 'doc',
                                    id: 'connectors/sinks/snowflake',
                                    label: 'Snowflake (experimental)'
                                }
                            ]
                        },
                    ],
                },
                {
                    type: 'category',
                    label: 'Formats',
                    link: {
                        type: 'doc',
                        id: 'formats/index'
                    },
                    items: [
                        'formats/json',
                        'formats/avro',
                        'formats/parquet',
                        'formats/csv',
                        'formats/raw'
                    ],
                },
                "fault-tolerance"
            ]
        }
        ,
        {
            type: 'category',
            label: 'Literature',
            items: ['papers', 'videos']
        }
    ]
}

module.exports = sidebars
