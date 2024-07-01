import { SystemError } from '$lib/services/pipelineManagerAggregateQuery'

import { expect, test } from '@playwright/experimental-ct-react'

import { HealthMenu } from './HealthPopup'

test.use({ viewport: { width: 800, height: 600 } })

test('Test display of errors', async ({ mount, page }) => {
  await mount(<HealthMenu systemErrors={systemErrors}></HealthMenu>)
  await expect(page).toHaveScreenshot('1-1.png')
  await page.getByTestId('button-expand').first().click()
  await expect(page).toHaveScreenshot('1-2.png')
  await page.getByTestId('button-expand').first().click()
  await expect(page).toHaveScreenshot('1-3.png')
})

const systemErrors: SystemError[] = [
  {
    name: 'Error in SQL code of sec-ops-program',
    message: 'Go to the source or expand to see the error',
    cause: {
      tag: 'programError',
      source: '/analytics/editor/?program_name=sec-ops-program',
      report: {
        '1-description':
          '```\nEncountered "not ," at line 5, column 30.\nWas expecting one of:\n    ")" ...\n    "," ...\n    "PRIMARY" ...\n    "FOREIGN" ...\n    "LATENESS" ...\n    "WATERMARK" ...\n    "DEFAULT" ...\n    "MULTISET" ...\n    "ARRAY" ...\n    "NULL" ...\n    "NOT" ...\n    "NOT" "NULL" ...\n    \n```',
        '2-repro':
          "<!-- Steps to reproduce the behavior e.g., Curl commands or steps in the UI such as:\n1. Go to '...'\n2. Click on '....'\n3. Scroll down to '....'\n4. See error -->",
        '3-expected': '<!-- A clear and concise description of what you expected to happen. -->',
        '4-screenshots': '<!-- If applicable, add screenshots/logs to help explain your problem. -->',
        '5-context':
          '\n - Feldera Version: <!-- [e.g. 0.12.0] -->\n - Environment: <!-- e.g. Docker, Cloud Sandbox, Native -->\n - Browser: <!-- chrome, safari -->\n',
        '6-extra':
          'SQL:\n```\n-- CI/CD pipeline.\ncreate table pipeline (\n    pipeline_id bigint not null,\n    create_date timestamp not null,\n    createdby_user_id bigint not ,\n    update_date timestamp,\n    updatedby_user_id bigint\n);\n\n-- Git commits used by each pipeline.\ncreate table pipeline_sources (\n    git_commit_id bigint not null,\n    pipeline_id bigint not null foreign key references pipeline(pipeline_id)\n);\n\n-- Binary artifact created by a CI pipeline.\ncreate table artifact (\n    artifact_id bigint not null,\n    artifact_uri varchar not null,\n    create_date timestamp not null,\n    createdby_user_id bigint not null,\n    checksum varchar not null,\n    checksum_type varchar not null,\n    artifact_size_in_bytes bigint not null,\n    artifact_type varchar not null,\n    builtby_pipeline_id bigint not null foreign key references pipeline(pipeline_id),\n    parent_artifact_id bigint foreign key references artifact(artifact_id)\n);\n\n-- Vulnerabilities discovered in source code.\ncreate table vulnerability (\n    vulnerability_id bigint not null,\n    discovery_date timestamp not null,\n    discovered_by_user_id bigint not null,\n    discovered_in bigint not null,\n    update_date timestamp,\n    updated_by_user_id bigint,\n    checksum varchar not null,\n    checksum_type varchar not null,\n    vulnerability_reference_id varchar not null,\n    severity int,\n    priority varchar\n);\n\n-- K8s clusters.\ncreate table k8scluster (\n    k8scluster_id bigint not null,\n    k8s_uri varchar not null,\n    name varchar not null,\n    k8s_service_provider varchar not null\n);\n\n\n-- Deployed k8s objects.\ncreate table k8sobject (\n    k8sobject_id bigint not null,\n    artifact_id bigint not null foreign key references artifact(artifact_id),\n    create_date timestamp not null,\n    createdby_user_id bigint not null,\n    update_date timestamp,\n    updatedby_user_id bigint,\n    checksum varchar not null,\n    checksum_type varchar not null,\n    deployed_id bigint not null foreign key references k8scluster(k8scluster_id),\n    deployment_type varchar not null,\n    k8snamespace varchar not null\n);\n\n-- Vulnerabilities that affect each pipeline.\ncreate view pipeline_vulnerability (\n    pipeline_id,\n    vulnerability_id\n) as\n    SELECT pipeline_sources.pipeline_id as pipeline_id, vulnerability.vulnerability_id as vulnerability_id FROM\n    pipeline_sources\n    INNER JOIN\n    vulnerability\n    ON pipeline_sources.git_commit_id = vulnerability.discovered_in;\n\n-- Vulnerabilities that affect each artifact.\ncreate view artifact_vulnerability (\n    artifact_id,\n    vulnerability_id\n) as\n    SELECT artifact.artifact_id as artifact_id, pipeline_vulnerability.vulnerability_id as vulnerability_id FROM\n    artifact\n    INNER JOIN\n    pipeline_vulnerability\n    ON artifact.builtby_pipeline_id = pipeline_vulnerability.pipeline_id;\n\n-- Vulnerabilities in the artifact or any of its children.\ncreate view transitive_artifact_vulnerability(\n    artifact_id,\n    via_artifact_id,\n    vulnerability_id\n) as\n    SELECT artifact_id, artifact_id as via_artifact_id, vulnerability_id from artifact_vulnerability\n    UNION\n    (\n        SELECT\n            artifact.parent_artifact_id as artifact_id,\n            artifact.artifact_id as via_artifact_id,\n            artifact_vulnerability.vulnerability_id as vulnerability_id FROM\n        artifact\n        INNER JOIN\n        artifact_vulnerability\n        ON artifact.artifact_id = artifact_vulnerability.artifact_id\n        WHERE artifact.parent_artifact_id IS NOT NULL\n    );\n\n-- Vulnerabilities that affect each k8s object.\ncreate view k8sobject_vulnerability (\n    k8sobject_id,\n    vulnerability_id\n) as\n    SELECT k8sobject.k8sobject_id, transitive_artifact_vulnerability.vulnerability_id FROM\n    k8sobject\n    INNER JOIN\n    transitive_artifact_vulnerability\n    ON k8sobject.artifact_id = transitive_artifact_vulnerability.artifact_id;\n\n-- Vulnerabilities that affect each k8s cluster.\ncreate view k8scluster_vulnerability (\n    k8scluster_id,\n    vulnerability_id\n) as\n    SELECT\n        k8sobject.deployed_id as k8scluster_id,\n        k8sobject_vulnerability.vulnerability_id FROM\n    k8sobject_vulnerability\n    INNER JOIN\n    k8sobject\n    ON k8sobject_vulnerability.k8sobject_id = k8sobject.k8sobject_id;\n\n-- Per-cluster statistics:\n-- * Number of vulnerabilities.\n-- * Most severe vulnerability.\ncreate view k8scluster_vulnerability_stats (\n    k8scluster_id,\n    k8scluster_name,\n    total_vulnerabilities,\n    most_severe_vulnerability\n) as\n    SELECT\n        cluster_id,\n        k8scluster.name as k8scluster_name,\n        total_vulnerabilities,\n        most_severe_vulnerability\n    FROM\n    (\n        SELECT\n            cluster_id,\n            COUNT(*) as total_vulnerabilities,\n            MAX(severity) as most_severe_vulnerability\n        FROM\n        (\n            SELECT k8scluster_vulnerability.k8scluster_id as cluster_id, vulnerability.vulnerability_id, vulnerability.severity FROM\n            k8scluster_vulnerability\n            INNER JOIN\n            vulnerability\n            ON k8scluster_vulnerability.vulnerability_id = vulnerability.vulnerability_id\n        )\n        GROUP BY cluster_id\n    )\n    INNER JOIN\n    k8scluster\n    ON k8scluster.k8scluster_id = cluster_id;\n\n```',
        name: 'Report: program compilation error'
      },
      body: 'Encountered "not ," at line 5, column 30.\nWas expecting one of:\n    ")" ...\n    "," ...\n    "PRIMARY" ...\n    "FOREIGN" ...\n    "LATENESS" ...\n    "WATERMARK" ...\n    "DEFAULT" ...\n    "MULTISET" ...\n    "ARRAY" ...\n    "NULL" ...\n    "NOT" ...\n    "NOT" "NULL" ...\n    '
    }
  },
  {
    name: 'Error running pipeline sec-ops-pipeline',
    message:
      "Operation failed because the pipeline failed to initialize. Error details: 'FATAL error on input endpoint 'secops_pipeline_sources': failed to subscribe to topics '[\"secops_pipeline_sources\"]' (consumer group id 'secops_pipeline_sources'), giving up after 10s'.",
    cause: {
      tag: 'pipelineError',
      source: '/streaming/builder/?pipeline_name=sec-ops-pipeline',
      report: {
        '1-description':
          "```\nOperation failed because the pipeline failed to initialize. Error details: 'FATAL error on input endpoint 'secops_pipeline_sources': failed to subscribe to topics '[\"secops_pipeline_sources\"]' (consumer group id 'secops_pipeline_sources'), giving up after 10s'.\n```",
        '2-repro':
          "<!-- Steps to reproduce the behavior e.g., Curl commands or steps in the UI such as:\n1. Go to '...'\n2. Click on '....'\n3. Scroll down to '....'\n4. See error -->",
        '3-expected': '<!-- A clear and concise description of what you expected to happen. -->',
        '4-screenshots': '<!-- If applicable, add screenshots/logs to help explain your problem. -->',
        '5-context':
          '\n - Feldera Version: <!-- [e.g. 0.12.0] -->\n - Environment: <!-- e.g. Docker, Cloud Sandbox, Native -->\n - Browser: <!-- chrome, safari -->\n',
        '6-extra':
          'Pipelince config:\n```\n{\n\t"workers": 8,\n\t"storage": false,\n\t"cpu_profiler": false,\n\t"tcp_metrics_exporter": false,\n\t"min_batch_size_records": 0,\n\t"max_buffering_delay_usecs": 0,\n\t"resources": {\n\t\t"cpu_cores_min": null,\n\t\t"cpu_cores_max": null,\n\t\t"memory_mb_min": null,\n\t\t"memory_mb_max": null,\n\t\t"storage_mb_max": null,\n\t\t"storage_class": null\n\t},\n\t"min_storage_rows": null\n}\n```\nSQL:\n```\n-- CI/CD pipeline.\ncreate table pipeline (\n    pipeline_id bigint not null,\n    create_date timestamp not null,\n    createdby_user_id bigint not ,\n    update_date timestamp,\n    updatedby_user_id bigint\n);\n\n-- Git commits used by each pipeline.\ncreate table pipeline_sources (\n    git_commit_id bigint not null,\n    pipeline_id bigint not null foreign key references pipeline(pipeline_id)\n);\n\n-- Binary artifact created by a CI pipeline.\ncreate table artifact (\n    artifact_id bigint not null,\n    artifact_uri varchar not null,\n    create_date timestamp not null,\n    createdby_user_id bigint not null,\n    checksum varchar not null,\n    checksum_type varchar not null,\n    artifact_size_in_bytes bigint not null,\n    artifact_type varchar not null,\n    builtby_pipeline_id bigint not null foreign key references pipeline(pipeline_id),\n    parent_artifact_id bigint foreign key references artifact(artifact_id)\n);\n\n-- Vulnerabilities discovered in source code.\ncreate table vulnerability (\n    vulnerability_id bigint not null,\n    discovery_date timestamp not null,\n    discovered_by_user_id bigint not null,\n    discovered_in bigint not null,\n    update_date timestamp,\n    updated_by_user_id bigint,\n    checksum varchar not null,\n    checksum_type varchar not null,\n    vulnerability_reference_id varchar not null,\n    severity int,\n    priority varchar\n);\n\n-- K8s clusters.\ncreate table k8scluster (\n    k8scluster_id bigint not null,\n    k8s_uri varchar not null,\n    name varchar not null,\n    k8s_service_provider varchar not null\n);\n\n\n-- Deployed k8s objects.\ncreate table k8sobject (\n    k8sobject_id bigint not null,\n    artifact_id bigint not null foreign key references artifact(artifact_id),\n    create_date timestamp not null,\n    createdby_user_id bigint not null,\n    update_date timestamp,\n    updatedby_user_id bigint,\n    checksum varchar not null,\n    checksum_type varchar not null,\n    deployed_id bigint not null foreign key references k8scluster(k8scluster_id),\n    deployment_type varchar not null,\n    k8snamespace varchar not null\n);\n\n-- Vulnerabilities that affect each pipeline.\ncreate view pipeline_vulnerability (\n    pipeline_id,\n    vulnerability_id\n) as\n    SELECT pipeline_sources.pipeline_id as pipeline_id, vulnerability.vulnerability_id as vulnerability_id FROM\n    pipeline_sources\n    INNER JOIN\n    vulnerability\n    ON pipeline_sources.git_commit_id = vulnerability.discovered_in;\n\n-- Vulnerabilities that affect each artifact.\ncreate view artifact_vulnerability (\n    artifact_id,\n    vulnerability_id\n) as\n    SELECT artifact.artifact_id as artifact_id, pipeline_vulnerability.vulnerability_id as vulnerability_id FROM\n    artifact\n    INNER JOIN\n    pipeline_vulnerability\n    ON artifact.builtby_pipeline_id = pipeline_vulnerability.pipeline_id;\n\n-- Vulnerabilities in the artifact or any of its children.\ncreate view transitive_artifact_vulnerability(\n    artifact_id,\n    via_artifact_id,\n    vulnerability_id\n) as\n    SELECT artifact_id, artifact_id as via_artifact_id, vulnerability_id from artifact_vulnerability\n    UNION\n    (\n        SELECT\n            artifact.parent_artifact_id as artifact_id,\n            artifact.artifact_id as via_artifact_id,\n            artifact_vulnerability.vulnerability_id as vulnerability_id FROM\n        artifact\n        INNER JOIN\n        artifact_vulnerability\n        ON artifact.artifact_id = artifact_vulnerability.artifact_id\n        WHERE artifact.parent_artifact_id IS NOT NULL\n    );\n\n-- Vulnerabilities that affect each k8s object.\ncreate view k8sobject_vulnerability (\n    k8sobject_id,\n    vulnerability_id\n) as\n    SELECT k8sobject.k8sobject_id, transitive_artifact_vulnerability.vulnerability_id FROM\n    k8sobject\n    INNER JOIN\n    transitive_artifact_vulnerability\n    ON k8sobject.artifact_id = transitive_artifact_vulnerability.artifact_id;\n\n-- Vulnerabilities that affect each k8s cluster.\ncreate view k8scluster_vulnerability (\n    k8scluster_id,\n    vulnerability_id\n) as\n    SELECT\n        k8sobject.deployed_id as k8scluster_id,\n        k8sobject_vulnerability.vulnerability_id FROM\n    k8sobject_vulnerability\n    INNER JOIN\n    k8sobject\n    ON k8sobject_vulnerability.k8sobject_id = k8sobject.k8sobject_id;\n\n-- Per-cluster statistics:\n-- * Number of vulnerabilities.\n-- * Most severe vulnerability.\ncreate view k8scluster_vulnerability_stats (\n    k8scluster_id,\n    k8scluster_name,\n    total_vulnerabilities,\n    most_severe_vulnerability\n) as\n    SELECT\n        cluster_id,\n        k8scluster.name as k8scluster_name,\n        total_vulnerabilities,\n        most_severe_vulnerability\n    FROM\n    (\n        SELECT\n            cluster_id,\n            COUNT(*) as total_vulnerabilities,\n            MAX(severity) as most_severe_vulnerability\n        FROM\n        (\n            SELECT k8scluster_vulnerability.k8scluster_id as cluster_id, vulnerability.vulnerability_id, vulnerability.severity FROM\n            k8scluster_vulnerability\n            INNER JOIN\n            vulnerability\n            ON k8scluster_vulnerability.vulnerability_id = vulnerability.vulnerability_id\n        )\n        GROUP BY cluster_id\n    )\n    INNER JOIN\n    k8scluster\n    ON k8scluster.k8scluster_id = cluster_id;\n\n```',
        name: 'Report: pipeline execution error'
      },
      body: {
        error: {
          backtrace:
            '   0: anyhow::error::<impl anyhow::Error>::msg\n   1: <dbsp_adapters::transport::kafka::nonft::input::KafkaInputEndpoint as dbsp_adapters::transport::TransportInputEndpoint>::open\n   2: dbsp_adapters::controller::ControllerInner::add_input_endpoint\n   3: dbsp_adapters::controller::ControllerInner::connect_input\n   4: dbsp_adapters::server::bootstrap\n   5: std::sys_common::backtrace::__rust_begin_short_backtrace\n   6: core::ops::function::FnOnce::call_once{{vtable.shim}}\n   7: std::sys::pal::unix::thread::Thread::new::thread_start\n   8: <unknown>\n   9: __clone\n',
          endpoint_name: 'secops_pipeline_sources',
          error:
            "failed to subscribe to topics '[\"secops_pipeline_sources\"]' (consumer group id 'secops_pipeline_sources'), giving up after 10s",
          fatal: true
        }
      }
    }
  },
  {
    name: '3 connector parse errors in sec-ops-pipeline',
    message: '3 parse errors in kafka_input connector pipeline_sources of sec-ops-pipeline',
    cause: {
      tag: 'xgressError',
      source: '/streaming/builder/?pipeline_name=sec-ops-pipeline',
      report: {
        '1-description': '<!-- A clear and concise description of what the bug is. -->',
        '2-repro':
          "<!-- Steps to reproduce the behavior e.g., Curl commands or steps in the UI such as:\n1. Go to '...'\n2. Click on '....'\n3. Scroll down to '....'\n4. See error -->",
        '3-expected': '<!-- A clear and concise description of what you expected to happen. -->',
        '4-screenshots': '<!-- If applicable, add screenshots/logs to help explain your problem. -->',
        '5-context':
          '\n - Feldera Version: <!-- [e.g. 0.12.0] -->\n - Environment: <!-- e.g. Docker, Cloud Sandbox, Native -->\n - Browser: <!-- chrome, safari -->\n',
        '6-extra':
          'Connector config:\n```\n{\n\t"connector_id": "01900649-1257-7c66-adc9-f01ac4240d5d",\n\t"name": "secops_pipeline",\n\t"description": "",\n\t"config": {\n\t\t"transport": {\n\t\t\t"name": "kafka_input",\n\t\t\t"config": {\n\t\t\t\t"auto.offset.reset": "earliest",\n\t\t\t\t"bootstrap.servers": "redpanda:9092",\n\t\t\t\t"topics": [\n\t\t\t\t\t"secops_pipeline"\n\t\t\t\t],\n\t\t\t\t"log_level": null,\n\t\t\t\t"group_join_timeout_secs": 10,\n\t\t\t\t"fault_tolerance": null\n\t\t\t}\n\t\t},\n\t\t"format": {\n\t\t\t"name": "json",\n\t\t\t"config": {\n\t\t\t\t"update_format": "insert_delete"\n\t\t\t}\n\t\t},\n\t\t"enable_output_buffer": false,\n\t\t"max_output_buffer_time_millis": 18446744073709552000,\n\t\t"max_output_buffer_size_records": 18446744073709552000,\n\t\t"max_queued_records": 1000000\n\t}\n}\n```\n',
        name: 'Report: kafka_input connector parse errors'
      },
      body: ''
    }
  }
]
