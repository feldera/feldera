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
    parser.add_argument('--delete-extra', default=False, action=argparse.BooleanOptionalAction, help='delete other programs, pipelines, and connectors (default: --no-delete-extra)')

    args = parser.parse_args()
    prepare_feldera(args.api_url, args.kafka_url_for_connector, args.registry_url_for_connector, args.delete_extra)
    prepare_redpanda_start_simulator("0" if args.prepare_args is None else args.prepare_args).wait()


PROGRAM_NAME = "sec-ops-program"
PIPELINE_NAME = "sec-ops-pipeline"
CONNECTORS = [
    ("secops_pipeline", 'PIPELINE', ["secops_pipeline"], True),
    ("secops_pipeline_sources", 'PIPELINE_SOURCES', ["secops_pipeline_sources"], True),
    ("secops_artifact", 'ARTIFACT', ["secops_artifact"], True),
    ("secops_vulnerability", 'VULNERABILITY', ["secops_vulnerability"], True),
    ("secops_cluster", 'K8SCLUSTER', ["secops_cluster"], True),
    ("secops_k8sobject", 'K8SOBJECT', ["secops_k8sobject"], True),
    ("secops_vulnerability_stats", 'K8SCLUSTER_VULNERABILITY_STATS', "secops_vulnerability_stats", False),
]


def wait_for_status(api_url, pipeline_name, status):
    start = time.time()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != status:
        time.sleep(.1)
    return time.time() - start

def list_names(api_url, entity):
    return set([entity["name"] for entity in requests.get(f"{api_url}/v0/{entity}").json()])

def list_programs(api_url):
    return list_names(api_url, "programs")

def list_pipelines(api_url):
    return set([pipeline["descriptor"]["name"] for pipeline in requests.get(f"{api_url}/v0/pipelines").json()])

def list_connectors(api_url):
    return list_names(api_url, "connectors")

def stop_pipeline(api_url, pipeline_name, wait):
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
    if wait:
        return wait_for_status(api_url, pipeline_name, "Shutdown")

def start_pipeline(api_url, pipeline_name, wait):
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start").raise_for_status()
    if wait:
        return wait_for_status(api_url, pipeline_name, "Running")

def delete_pipeline(api_url, pipeline_name):
    requests.delete(f"{api_url}/v0/pipelines/{pipeline_name}").raise_for_status()

def delete_connector(api_url, connector_name):
    requests.delete(f"{api_url}/v0/connectors/{connector_name}").raise_for_status()

def delete_program(api_url, program_name):
    requests.delete(f"{api_url}/v0/programs/{program_name}").raise_for_status()


def prepare_feldera(api_url, pipeline_to_redpanda_server, pipeline_to_schema_registry, delete_extra):
    if delete_extra:
        for pipeline in list_pipelines(api_url):
            stop_pipeline(api_url, pipeline, False)
        for pipeline in list_pipelines(api_url):
            stop_pipeline(api_url, pipeline, True)
        for pipeline in list_pipelines(api_url) - set([PIPELINE_NAME]):
            delete_pipeline(api_url, pipeline)
        for program in list_programs(api_url) - set([PROGRAM_NAME]):
            delete_program(api_url, program)
        for connector in list_connectors(api_url) - set([c[0] for c in CONNECTORS]):
            delete_connector(api_url, connector)

    # Create program
    program_sql = open(PROJECT_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{PROGRAM_NAME}", json={
        "description": "",
        "code": program_sql
    })
    response.raise_for_status()
    program_version = response.json()["version"]

    # Compile program
    print(f"Compiling program {PROGRAM_NAME} (version: {program_version})...")
    requests.post(f"{api_url}/v0/programs/{PROGRAM_NAME}/compile", json={"version": program_version}).raise_for_status()
    while True:
        status = requests.get(f"{api_url}/v0/programs/{PROGRAM_NAME}").json()["status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(5)

    # Connectors
    connectors = []
    for (connector_name, stream, topic_topics, is_input) in CONNECTORS:
        requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
            "description": "",
            "config": {
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "insert_delete"
                    }
                },
                "transport": {
                    "name": "kafka_" + ("input" if is_input else "output"),
                    "config": {
                        "bootstrap.servers": pipeline_to_redpanda_server,
                        "topic": topic_topics
                    } if not is_input else (
                        {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topics": topic_topics,
                            "auto.offset.reset": "earliest",
                            "group.id": "secops_pipeline_sources",
                            "enable.auto.commit": "true",
                            "enable.auto.offset.store": "true",
                        }
                        if stream == "PIPELINE_SOURCES" else
                        {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topics": topic_topics,
                            "auto.offset.reset": "earliest"
                        }
                    )
                }
            }
        })
        connectors.append({
            "connector_name": connector_name,
            "is_input": is_input,
            "name": connector_name,
            "relation_name": stream
        })

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

    if pipeline_to_schema_registry:
        requests.put(f"{api_url}/v0/connectors/secops_vulnerability_stats_avro", json={
            "description": "",
            "config": {
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
            }
        })
        connectors.append({
            "connector_name": "secops_vulnerability_stats_avro",
            "is_input": False,
            "name": "secops_vulnerability_stats_avro",
            "relation_name": "k8scluster_vulnerability_stats"
        })

    # Create pipeline
    requests.put(f"{api_url}/v0/pipelines/{PIPELINE_NAME}", json={
        "description": "",
        "config": {"workers": 8},
        "program_name": PROGRAM_NAME,
        "connectors": connectors,
    }).raise_for_status()


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
