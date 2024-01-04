import time
import requests
import argparse
from typing import List
import os
import subprocess
from shutil import which
from plumbum.cmd import rpk

# Required files
DEMO_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(DEMO_DIR, "project.sql")


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    parser.add_argument("--num-pipelines", required=False, help="number of SecOps pipelines to simulate")
    parsed_args = parser.parse_args()
    api_url = parsed_args.api_url
    num_pipelines = "-1" if parsed_args.num_pipelines is None else parsed_args.num_pipelines
    pipeline_to_redpanda_server = "redpanda:9092"

    # Program
    program_name = "demo-sec-ops-program"
    program_sql = open(PROJECT_SQL).read()
    program_version = create_program(api_url, {}, program_sql, program_name)
    compile_program(api_url, {}, program_name, program_version)

    # Connectors
    connectors = []
    for (name, stream, topic_topics, is_input) in [
        ("secops_pipeline", 'PIPELINE',  ["secops_pipeline"], True),
        ("secops_pipeline_sources", 'PIPELINE_SOURCES', ["secops_pipeline_sources"], True),
        ("secops_artifact", 'ARTIFACT', ["secops_artifact"], True),
        ("secops_vulnerability", 'VULNERABILITY', ["secops_vulnerability"], True),
        ("secops_cluster", 'K8SCLUSTER', ["secops_cluster"], True),
        ("secops_k8sobject", 'K8SOBJECT', ["secops_k8sobject"], True),
        ("secops_vulnerability_stats", 'K8SCLUSTER_VULNERABILITY_STATS', "secops_vulnerability_stats", False),
    ]:
        create_connector(api_url, {}, name, {
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete"
                }
            },
            "transport": {
                "name": "kafka",
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
        })
        connectors.append({
            "connector_name": name,
            "is_input": is_input,
            "name": name,
            "relation_name": stream
        })

    # Pipeline
    pipeline_name = "demo-sec-ops-pipeline"
    create_pipeline(
        api_url,
        {},
        program_name,
        pipeline_name,
        num_workers=8,
        connectors=connectors
    )

    # Create output topic before running the simulator, which will never return.
    print("(Re-)creating topic secops_vulnerability_stats...")
    rpk["topic", "delete", "secops_vulnerability_stats"]()
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
    if which("cargo") is None:
        # Expect a pre-built binary in simulator/secops_simulator. Used
        # by the Docker container workflow where we don't want to use cargo run.
        cmd = ["./secops_simulator", "%s" % num_pipelines]
        subprocess.run(cmd, cwd=os.path.join(DEMO_DIR, "simulator"))
    else:
        cmd = ["cargo", "run", "--release", "--", "%s" % num_pipelines]
        # Override --release if RUST_BUILD_PROFILE is set
        if "RUST_BUILD_PROFILE" in os.environ:
            cmd[2] = os.environ["RUST_BUILD_PROFILE"]
        new_env = os.environ.copy()
        new_env["RUST_LOG"] = "debug"
        subprocess.run(cmd, cwd=os.path.join(DEMO_DIR, "simulator"), env=new_env)


###############################################################################
# HELPER FUNCTIONS BELOW


def create_program(
        api_url: str,
        headers: dict,
        program_sql: str,
        program_name: str,
        program_description: str = ""
) -> str:
    """
    Creates a Feldera program. Patches existing program if exists, else posts.
    Function version: 0.0.1

    :param api_url:               URL of the Feldera REST API (e.g., "http://localhost:8080")
    :param headers:               Headers to add to each HTTP request
    :param program_sql:           Program SQL
    :param program_name:          Program name
    :param program_description:   (Optional) Program description

    :return: Program version
    """
    print(f"Creating program {program_name}...")
    program = {
        "name": program_name,
        "description": program_description,
        "code": program_sql,
    }
    existing_programs = [entry["name"] for entry in requests.get(api_url + "/v0/programs", headers=headers).json()]
    if program["name"] in existing_programs:
        response = requests.patch(f"{api_url}/v0/programs/{program_name}", json=program, headers=headers)
    else:
        response = requests.post(f"{api_url}/v0/programs", json=program, headers=headers)
    response.raise_for_status()
    program_version = response.json()["version"]
    print(f"Created program {program_name} (version: {program_version})")
    return program_version


def compile_program(
        api_url: str,
        headers: dict,
        program_name: str,
        program_version: str
):
    """
    Compiles a Feldera program (returns when compilation is completed).
    Function version: 0.0.1

    :param api_url:               URL of the Feldera REST API (e.g., "http://localhost:8080")
    :param headers:               Headers to add to each HTTP request
    :param program_name:          Program name
    :param program_version:       Program version
    """
    print("Starting program compilation...")
    requests.post(
        f"{api_url}/v0/programs/{program_name}/compile", json={"version": program_version}, headers=headers
    ).raise_for_status()
    while True:
        status = requests.get(f"{api_url}/v0/programs/{program_name}", headers=headers).json()["status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            print(f"Failed program compilation with status {status}")
            exit(1)
        time.sleep(5)
    print(f"Program {program_name} is compiled")


def create_connector(
        api_url: str,
        headers: dict,
        connector_name: str,
        connector_config: dict,
        connector_description: str = "",
):
    """
    Creates a connector provided its configuration.
    Function version: 0.0.1

    :param api_url:                URL of the Feldera REST API (e.g., "http://localhost:8080")
    :param headers:                Headers to add to each HTTP request
    :param connector_name:         Connector name
    :param connector_config:       Connector configuration
    :param connector_description:  (Optional) Connector description
    """
    connector = {
        "name": connector_name,
        "description": connector_description,
        "config": connector_config,
    }
    print(f"Creating connector {connector_name}...")
    existing_connectors = [entry["name"] for entry in requests.get(f"{api_url}/v0/connectors").json()]
    if connector_name in existing_connectors:
        response = requests.patch(f"{api_url}/v0/connectors/{connector_name}", json=connector, headers=headers)
    else:
        response = requests.post(f"{api_url}/v0/connectors", json=connector, headers=headers)
    response.raise_for_status()
    print(f"Created connector {connector_name}")


def create_pipeline(
        api_url: str,
        headers: dict,
        program_name: str,
        pipeline_name: str,
        pipeline_description: str = "",
        num_workers: int = 8,
        connectors: List[dict] = None
):
    """
    Creates a pipeline provided the program. Patches existing pipeline if exists, else posts.
    Function version: 0.0.1

    :param api_url:               URL of the Feldera REST API (e.g., "http://localhost:8080")
    :param headers:               Headers to add to each HTTP request
    :param program_name:          Program name
    :param pipeline_name:         Pipeline name
    :param pipeline_description:  (Optional) Pipeline description
    :param num_workers:           (Optional) Number of workers (default: 8)
    :param connectors:            (Optional) List of connector names to attach to the pipeline
    """
    pipeline = {
        "name": pipeline_name,
        "description": pipeline_description,
        "config": {"workers": num_workers},
        "program_name": program_name,
        "connectors": [] if connectors is None else connectors,
    }
    print(f"Creating pipeline {pipeline_name}...")
    existing_pipelines = [
        pipeline["descriptor"]["name"]
        for pipeline in requests.get(api_url + "/v0/pipelines", headers=headers).json()
    ]
    if pipeline["name"] in existing_pipelines:
        response = requests.patch(f"{api_url}/v0/pipelines/{pipeline_name}", json=pipeline, headers=headers)
    else:
        response = requests.post(f"{api_url}/v0/pipelines", json=pipeline, headers=headers)
    response.raise_for_status()
    print(f"Created pipeline {pipeline_name}")


if __name__ == "__main__":
    main()
