import time
import os
import requests
import argparse
from typing import List

# File locations
DEMO_DIR = os.path.join(os.path.dirname(__file__))
RECORDS_CSV = os.path.join(DEMO_DIR, "records.csv")
MESSAGES_CSV = os.path.join(DEMO_DIR, "messages.csv")
COMBINER_SQL = os.path.join(DEMO_DIR, "combiner.sql")
MATCHES_CSV = os.path.join(DEMO_DIR, "matches.csv")


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    api_url = parser.parse_args().api_url

    # Required files
    if not os.path.exists(MESSAGES_CSV):
        raise Exception('Input CSV file {} not found', MESSAGES_CSV)
    if not os.path.exists(RECORDS_CSV):
        raise Exception('Input CSV file {} not found', RECORDS_CSV)
    if not os.path.exists(COMBINER_SQL):
        raise Exception('Input SQL file {} not found', COMBINER_SQL)
    with open(MATCHES_CSV, "w+") as f_out:
        f_out.write("")

    # Program
    program_name = "demo-hello-world-program"
    program_sql = open(COMBINER_SQL).read()
    program_version = create_program(api_url, {}, program_sql, program_name)
    compile_program(api_url, {}, program_name, program_version)

    # Connectors
    connectors = []
    for (name, stream, filepath, is_input) in [
        ("demo-hello-world-messages", "MESSAGES", MESSAGES_CSV, True),
        ("demo-hello-world-records", "RECORDS", RECORDS_CSV, True),
        ("demo-hello-world-matches", "MATCHES", MATCHES_CSV, False),
    ]:

        # Create connector
        create_connector(
            api_url, {}, name,
            {
                "transport": {
                    "name": "file",
                    "config": {
                        "path": filepath,
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                }
            }
        )

        # Add to list of connectors
        connectors.append({
            "connector_name": name,
            "is_input": is_input,
            "name": name,
            "relation_name": stream
        })

    # Pipeline
    pipeline_name = "demo-hello-world-pipeline"
    create_pipeline(
        api_url,
        {},
        program_name,
        pipeline_name,
        num_workers=6,
        connectors=connectors
    )
    start_pipeline(api_url, {}, pipeline_name)

    # Wait till the pipeline is completed
    while not requests.get(f"{api_url}/v0/pipelines/{pipeline_name}/stats") \
            .json()["global_metrics"]["pipeline_complete"]:
        time.sleep(1)
    print("Pipeline finished")

    # Verify result
    with open(MATCHES_CSV) as f:
        content = f.read().strip()
        assert content == "Hello world!,1"
    print("Demo success: hello-world")


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


def start_pipeline(
        api_url: str,
        headers: dict,
        pipeline_name: str,
):
    """
    Starts a pipeline. Makes sure it is first shutdown before starting.
    Only returns when the pipeline has started.
    Function version: 0.0.1

    :param api_url:               URL of the Feldera REST API (e.g., "http://localhost:8080")
    :param headers:               Headers to add to each HTTP request
    :param pipeline_name:         Pipeline name
    """
    print(f"Starting pipeline {pipeline_name}...")
    get_response = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}", headers=headers)
    get_response.raise_for_status()
    if get_response.json()["state"]["current_status"] != "Shutdown":
        requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown", headers=headers).raise_for_status()
    print("Awaiting pipeline shutdown...")
    while requests.get(
            f"{api_url}/v0/pipelines/{pipeline_name}", headers=headers
    ).json()["state"]["current_status"] != "Shutdown":
        time.sleep(1)
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start", headers=headers).raise_for_status()
    print("Awaiting pipeline running...")
    while requests.get(
            f"{api_url}/v0/pipelines/{pipeline_name}", headers=headers
    ).json()["state"]["current_status"] != "Running":
        time.sleep(1)
    print(f"Started pipeline {pipeline_name}")


if __name__ == "__main__":
    main()
