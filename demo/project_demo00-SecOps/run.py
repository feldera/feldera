# Start the demo locally (without docker):
#
# python3 ./run.py  --api-url http://localhost:8080 --kafka-url-for-connector=localhost:19092 --registry-url-for-connector=http://localhost:18081
#
# (the last two arguments shouldn't be needed in docker compose)
#
# Tail the topic with Avro output using kcat:
# kcat -b localhost:19092 -t secops_vulnerability_stats_avro -r localhost:18081 -s value=avro

import os
import time
import requests
import argparse
import subprocess
import json
from shutil import which
from plumbum.cmd import rpk

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    default_api_url = "http://localhost:8080"
    parser.add_argument("--api-url", default=default_api_url, help=f"Feldera API URL (default: {default_api_url})")
    parser.add_argument("--prepare-args", required=False, help="number of SecOps pipelines to simulate")
    parser.add_argument("--kafka-url-for-connector", required=False, default="redpanda:9092",
                        help="Kafka URL from pipeline")
    parser.add_argument("--registry-url-for-connector", required=False, default="http://redpanda:8081",
                        help="Schema registry URL from pipeline")

    args = parser.parse_args()
    prepare_feldera(args.api_url, args.kafka_url_for_connector, args.registry_url_for_connector)
    prepare_redpanda_start_simulator("0" if args.prepare_args is None else args.prepare_args).wait()


PIPELINE_NAME = "sec-ops-pipeline"

def make_connector(topic, pipeline_to_redpanda_server, input=True, group_id=None):
    if input:
        name = "kafka_input"
        config = {"topics": [topic], "auto.offset.reset": "earliest"}
        if group_id is not None:
            config["group.id"] = "secops_pipeline_sources"
            config["enable.auto.commit"] = "true"
            config["enable.auto.offset.store"] = "true"
    else:
        name = "kafka_output"
        config = {"topic": topic}
    config["bootstrap.servers"] = pipeline_to_redpanda_server

    return [{
        "format": {
            "name": "json",
            "config": {
                "update_format": "insert_delete"
            }
        },
        "transport": {
            "name": name,
            "config": config
        }
    }]

def build_sql(pipeline_to_redpanda_server, pipeline_to_schema_registry):
    subst = {}
    for topic in ("secops_pipeline",
                  "secops_artifact",
                  "secops_vulnerability",
                  "secops_cluster",
                  "secops_k8sobject"):
        subst[topic] = make_connector(topic, pipeline_to_redpanda_server)

    for topic in ('secops_pipeline_sources',):
        subst[topic] = make_connector(topic, pipeline_to_redpanda_server, group_id=topic)

    for topic in ("secops_vulnerability_stats",):
        connector = make_connector(topic, pipeline_to_redpanda_server, input=False)
        if pipeline_to_schema_registry:
            schema = """{
                    "type": "record",
                    "name": "k8scluster_vulnerability_stats",
                    "fields": [
                        { "name": "k8scluster_id", "type": "long" },
                        { "name": "k8scluster_name", "type": "string" },
                        { "name": "total_vulnerabilities", "type": "long" },
                        { "name": "most_severe_vulnerability", "type": ["null","int"] }
                    ]
                }"""
            connector += [{
                "format": {
                    "name": "avro",
                    "config": {
                        "schema": schema,
                        "registry_urls": [pipeline_to_schema_registry],
                    }
                },
                "transport": {
                    "name": "kafka_output",
                    "config": {
                        "bootstrap.servers": pipeline_to_redpanda_server,
                        "topic": "secops_vulnerability_stats_avro",
                        "headers": [{"key": "header1", "value": "this is a string"},
                                    {"key": "header2", "value": list(b'byte array')}]
                    }
                }
            }]
        subst[topic] = connector

    for key in subst.keys():
        subst[key] = json.dumps(subst[key], indent=4)


    return """-- CI/CD pipeline.
create table pipeline (
    pipeline_id bigint not null primary key,
    create_date timestamp not null,
    createdby_user_id bigint not null,
    update_date timestamp,
    updatedby_user_id bigint
) WITH ('connectors' = '{secops_pipeline}');

-- Git commits used by each pipeline.
create table pipeline_sources (
    git_commit_id bigint not null,
    pipeline_id bigint not null foreign key references pipeline(pipeline_id)
) WITH ('connectors' = '{secops_pipeline_sources}');

-- Binary artifact created by a CI pipeline.
create table artifact (
    artifact_id bigint not null primary key,
    artifact_uri varchar not null,
    create_date timestamp not null,
    createdby_user_id bigint not null,
    checksum varchar not null,
    checksum_type varchar not null,
    artifact_size_in_bytes bigint not null,
    artifact_type varchar not null,
    builtby_pipeline_id bigint not null foreign key references pipeline(pipeline_id),
    parent_artifact_id bigint foreign key references artifact(artifact_id)
) WITH ('connectors' = '{secops_artifact}');

-- Vulnerabilities discovered in source code.
create table vulnerability (
    vulnerability_id bigint not null,
    discovery_date timestamp not null,
    discovered_by_user_id bigint not null,
    discovered_in bigint not null,
    update_date timestamp,
    updated_by_user_id bigint,
    checksum varchar not null,
    checksum_type varchar not null,
    vulnerability_reference_id varchar not null,
    severity int,
    priority varchar
)
  -- Instruct Feldera to store the snapshot of the table, allowing the
  -- user to browse it via the UI or API.
  with ('materialized' = 'true', 'connectors' = '{secops_vulnerability}');

-- K8s clusters.
create table k8scluster (
    k8scluster_id bigint not null primary key,
    k8s_uri varchar not null,
    name varchar not null,
    k8s_service_provider varchar not null
) WITH ('connectors' = '{secops_cluster}');


-- Deployed k8s objects.
create table k8sobject (
    k8sobject_id bigint not null,
    artifact_id bigint not null foreign key references artifact(artifact_id),
    create_date timestamp not null,
    createdby_user_id bigint not null,
    update_date timestamp,
    updatedby_user_id bigint,
    checksum varchar not null,
    checksum_type varchar not null,
    deployed_id bigint not null foreign key references k8scluster(k8scluster_id),
    deployment_type varchar not null,
    k8snamespace varchar not null
) WITH ('connectors' = '{secops_k8sobject}');

-- Vulnerabilities that affect each pipeline.
create view pipeline_vulnerability (
    pipeline_id,
    vulnerability_id
) as
    SELECT pipeline_sources.pipeline_id as pipeline_id, vulnerability.vulnerability_id as vulnerability_id FROM
    pipeline_sources
    INNER JOIN
    vulnerability
    ON pipeline_sources.git_commit_id = vulnerability.discovered_in;

-- Vulnerabilities that affect each artifact.
create view artifact_vulnerability (
    artifact_id,
    vulnerability_id
) as
    SELECT artifact.artifact_id as artifact_id, pipeline_vulnerability.vulnerability_id as vulnerability_id FROM
    artifact
    INNER JOIN
    pipeline_vulnerability
    ON artifact.builtby_pipeline_id = pipeline_vulnerability.pipeline_id;

-- Vulnerabilities in the artifact or any of its children.
create view transitive_artifact_vulnerability(
    artifact_id,
    via_artifact_id,
    vulnerability_id
) as
    SELECT artifact_id, artifact_id as via_artifact_id, vulnerability_id from artifact_vulnerability
    UNION
    (
        SELECT
            artifact.parent_artifact_id as artifact_id,
            artifact.artifact_id as via_artifact_id,
            artifact_vulnerability.vulnerability_id as vulnerability_id FROM
        artifact
        INNER JOIN
        artifact_vulnerability
        ON artifact.artifact_id = artifact_vulnerability.artifact_id
        WHERE artifact.parent_artifact_id IS NOT NULL
    );

-- Vulnerabilities that affect each k8s object.
create view k8sobject_vulnerability (
    k8sobject_id,
    vulnerability_id
) as
    SELECT k8sobject.k8sobject_id, transitive_artifact_vulnerability.vulnerability_id FROM
    k8sobject
    INNER JOIN
    transitive_artifact_vulnerability
    ON k8sobject.artifact_id = transitive_artifact_vulnerability.artifact_id;

-- Vulnerabilities that affect each k8s cluster.
create view k8scluster_vulnerability (
    k8scluster_id,
    vulnerability_id
) as
    SELECT
        k8sobject.deployed_id as k8scluster_id,
        k8sobject_vulnerability.vulnerability_id FROM
    k8sobject_vulnerability
    INNER JOIN
    k8sobject
    ON k8sobject_vulnerability.k8sobject_id = k8sobject.k8sobject_id;

-- Per-cluster statistics:
-- * Number of vulnerabilities.
-- * Most severe vulnerability.
create materialized view k8scluster_vulnerability_stats (
    k8scluster_id,
    k8scluster_name,
    total_vulnerabilities,
    most_severe_vulnerability
) WITH ('connectors' = '{secops_vulnerability_stats}') AS
    SELECT
        cluster_id,
        k8scluster.name as k8scluster_name,
        total_vulnerabilities,
        most_severe_vulnerability
    FROM
    (
        SELECT
            cluster_id,
            COUNT(*) as total_vulnerabilities,
            MAX(severity) as most_severe_vulnerability
        FROM
        (
            SELECT k8scluster_vulnerability.k8scluster_id as cluster_id, vulnerability.vulnerability_id, vulnerability.severity FROM
            k8scluster_vulnerability
            INNER JOIN
            vulnerability
            ON k8scluster_vulnerability.vulnerability_id = vulnerability.vulnerability_id
        )
        GROUP BY cluster_id
    )
    INNER JOIN
    k8scluster
    ON k8scluster.k8scluster_id = cluster_id;
""".format(**subst)


def prepare_feldera(api_url, pipeline_to_redpanda_server, pipeline_to_schema_registry):
    # Create pipeline
    requests.put(f"{api_url}/v0/pipelines/{PIPELINE_NAME}", json={
        "name": PIPELINE_NAME,
        "description": "Developer security operations demo",
        "runtime_config": {"workers": 8},
        "program_config": {},
        "program_code": build_sql(pipeline_to_redpanda_server, pipeline_to_schema_registry),
    }).raise_for_status()

    # Compile program
    print("Compiling program ...")
    while True:
        status = requests.get(f"{api_url}/v0/pipelines/{PIPELINE_NAME}").json()["program_status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(2)


def prepare_redpanda_start_simulator(num_pipelines, quiet=False):
    # Create output topic before starting the simulator.
    print("(Re-)creating topic secops_vulnerability_stats...")
    rpk["topic", "delete", "secops_vulnerability_stats"]()
    rpk["topic", "delete", "secops_vulnerability_stats_avro"]()
    rpk[
        "topic",
        "create",
        "secops_vulnerability_stats",
        "-c",
        "retention.ms=-1",
        "-c",
        "retention.bytes=100000000",
    ]()
    print("(Re-)created topic secops_vulnerability_stats")

    # Start running the simulator
    kwargs = { 'cwd': os.path.join(SCRIPT_DIR, "simulator") }
    if quiet:
        kwargs['stdout'] = subprocess.DEVNULL
        kwargs['stderr'] = subprocess.DEVNULL
    if which("cargo") is None:
        # Expect a pre-built binary in simulator/secops_simulator. Used
        # by the Docker container workflow where we don't want to use cargo run.
        cmd = ["./secops_simulator", "%s" % num_pipelines]
    else:
        cmd = ["cargo", "run", "--release", "--", "%s" % num_pipelines]
        # Override --release if RUST_BUILD_PROFILE is set
        if "RUST_BUILD_PROFILE" in os.environ:
            cmd[2] = os.environ["RUST_BUILD_PROFILE"]
        new_env = os.environ.copy()
        new_env["RUST_LOG"] = "debug"
        kwargs['env'] = new_env
    return subprocess.Popen(cmd, **kwargs)


if __name__ == "__main__":
    main()
