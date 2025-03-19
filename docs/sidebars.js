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

// Installation section
const installation = {
    type: 'category',
    label: 'Install Feldera',
    link: {
        type: 'doc',
        id: 'get-started/index'
    },
    items: [
        'get-started/docker',
        'get-started/sandbox',
        {
            type: 'category',
            label: 'Feldera Enterprise',
            link: {
                type: 'doc',
                id: 'get-started/enterprise/index'
            },
            items: [
                'get-started/enterprise/quickstart',
                'get-started/enterprise/helm-guide',
                {
                    type: 'category',
                    label: 'Kubernetes guides',
                    link: {
                        type: 'doc',
                        id: 'get-started/enterprise/kubernetes-guides/index',
                    },
                    items: [
                        'get-started/enterprise/kubernetes-guides/k3d',
                        {
                            type: 'category',
                            label: 'EKS',
                            link: {
                                type: 'doc',
                                id: 'get-started/enterprise/kubernetes-guides/eks/index',
                            },
                            items: [
                                'get-started/enterprise/kubernetes-guides/eks/cluster',
                                'get-started/enterprise/kubernetes-guides/eks/ingress'
                            ]
                        },
                        'get-started/enterprise/kubernetes-guides/secret-management'
                    ]
                }
            ]
        }
    ]
};

// Guides section
const guides = {
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
            type: 'category',
            label: 'Accelerating Batch Analytics',
            link: {
                type: 'doc',
                id: 'use_cases/batch/intro'
            },
            items: [
                'use_cases/batch/part1',
                'use_cases/batch/part2'
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
};

// Pipeline interface section
const interface = {
    type: 'category',
    label: 'Feldera Interface',
    collapsed: false,
    link: {
        type: 'doc',
        id: 'interface/index'
    },
    items: [
        'interface/web-console',
        'interface/cli',
        {
            type: 'link',
            label: "Python SDK",
            href: "pathname:///python/",
        },
        {
            type: 'link',
            label: "REST API",
            href: "pathname:///api",
        }
    ]
};

// SQL section
const sql = {
    type: 'category',
    label: 'Feldera SQL',
    link: {
        type: 'doc',
        id: 'pipelines/sql/index'
    },
    items: [
        'pipelines/sql/grammar',
        'pipelines/sql/identifiers',
        'pipelines/sql/types',
        'pipelines/sql/casts',
        'pipelines/sql/operators',
        'pipelines/sql/aggregates',
        'pipelines/sql/boolean',
        'pipelines/sql/comparisons',
        'pipelines/sql/integer',
        'pipelines/sql/float',
        'pipelines/sql/decimal',
        'pipelines/sql/string',
        'pipelines/sql/uuid',
        'pipelines/sql/binary',
        'pipelines/sql/array',
        'pipelines/sql/map',
        'pipelines/sql/datetime',
        'pipelines/sql/json',
        'pipelines/sql/materialized',
        'pipelines/sql/recursion',
        'pipelines/sql/ad-hoc',
        'pipelines/sql/streaming',
        'pipelines/sql/system',
        'pipelines/sql/table',
        'pipelines/sql/udf',
        'pipelines/sql/function-index'
    ]
};

// Connectors section
const connectors = {
    type: 'category',
    label: 'Connectors',
    link: {
        type: 'doc',
        id: 'pipelines/connectors/index'
    },
    items: [
        'pipelines/connectors/orchestration',
        {
            type: 'category',
            label: 'Input',
            link: {
                type: 'doc',
                id: 'pipelines/connectors/sources/index',
            },
            items: [
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/http',
                    label: 'HTTP'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/http-get',
                    label: 'HTTP GET (URL)'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/delta',
                    label: 'Delta Lake'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/iceberg',
                    label: 'Apache Iceberg'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/kafka',
                    label: 'Kafka'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/pubsub',
                    label: 'Google Pub/Sub'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/debezium',
                    label: 'Debezium'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/postgresql',
                    label: 'PostgreSQL'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/s3',
                    label: 'AWS S3'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sources/datagen',
                    label: 'Data Generator'
                }
            ]
        },
        {
            type: 'category',
            label: 'Output',
            link: {
                type: 'doc',
                id: 'pipelines/connectors/sinks/index',
            },
            items: [
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sinks/http',
                    label: 'HTTP'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sinks/delta',
                    label: 'Delta Lake'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sinks/kafka',
                    label: 'Kafka'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sinks/confluent-jdbc',
                    label: 'Confluent JDBC Connector'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sinks/redis',
                    label: 'Redis'
                },
                {
                    type: 'doc',
                    id: 'pipelines/connectors/sinks/snowflake',
                    label: 'Snowflake (experimental)'
                }
            ]
        }
    ]
};

// Formats section
const formats = {
    type: 'category',
    label: 'Formats',
    link: {
        type: 'doc',
        id: 'pipelines/formats/index'
    },
    items: [
        'pipelines/formats/json',
        'pipelines/formats/avro',
        'pipelines/formats/parquet',
        'pipelines/formats/csv',
        'pipelines/formats/raw'
    ]
};

// Pipelines section
const pipelines = {
    type: 'category',
    label: 'Pipelines',
    collapsed: false,
    link: {
        type: 'doc',
        id: 'pipelines/index'
    },
    items: [
        sql,
        connectors,
        formats,
        "pipelines/fault-tolerance"
    ]
};

// Literature section
const literature = {
    type: 'category',
    label: 'Literature',
    items: [
        'literature/papers',
        'literature/videos'
    ]
};

// Combine all sections
const sidebars = {
    docsSidebar: [
        'what-is-feldera',
        installation,
        guides,
        interface,
        pipelines,
        literature
    ]
};

module.exports = sidebars;
