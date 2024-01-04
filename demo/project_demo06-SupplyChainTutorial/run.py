import os
import time
import requests
import argparse
from typing import List
from plumbum.cmd import rpk

# File locations
DEMO_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(DEMO_DIR, "project.sql")


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    parsed_args = parser.parse_args()
    api_url = parsed_args.api_url
    pipeline_to_redpanda_server = "redpanda:9092"

    # Kafka topics
    print("(Re-)creating topics price and preferred_vendor...")
    rpk["topic", "delete", "price"]()
    rpk["topic", "create", "price"]()
    rpk["topic", "delete", "preferred_vendor"]()
    rpk["topic", "create", "preferred_vendor"]()
    print("(Re-)created topics price and preferred_vendor")

    # Program
    program_name = "demo-supply-chain-tutorial-program"
    program_sql = open(PROJECT_SQL).read()
    program_version = create_program(api_url, {}, program_sql, program_name,
                                     program_description="Supply Chain Tutorial demo program")
    compile_program(api_url, {}, program_name, program_version)

    # Connectors
    connectors = []
    for (name, stream, config, is_input) in [
        ("tutorial-part-s3", 'PART', {
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete"
                }
            },
            "transport": {
                "name": "url",
                "config": {
                    "path": "https://feldera-basics-tutorial.s3.amazonaws.com/part.json"
                }
            }
        }, True),
        ("tutorial-vendor-s3", 'VENDOR', {
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete"
                }
            },
            "transport": {
                "name": "url",
                "config": {
                    "path": "https://feldera-basics-tutorial.s3.amazonaws.com/vendor.json"
                }
            }
        }, True),
        ("tutorial-price-s3", 'PRICE', {
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete"
                }
            },
            "transport": {
                "name": "url",
                "config": {
                    "path": "https://feldera-basics-tutorial.s3.amazonaws.com/price.json"
                }
            }
        }, True),
        ("tutorial-price-redpanda", 'PRICE', {
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete"
                }
            },
            "transport": {
                "name": "kafka",
                "config": {
                    "topics": ["price"],
                    "bootstrap.servers": pipeline_to_redpanda_server,
                    "auto.offset.reset": "earliest",
                    "group.id": "tutorial-price",
                }
            }
        }, True),
        ("tutorial-preferred_vendor-redpanda", 'PREFERRED_VENDOR', {
            "format": {
                "name": "json",
                "config": {
                    "update_format": "insert_delete"
                }
            },
            "transport": {
                "name": "kafka",
                "config": {
                    "topic": "preferred_vendor",
                    "bootstrap.servers": pipeline_to_redpanda_server,
                }
            }
        }, False),
    ]:
        create_connector(api_url, {}, name, config)
        connectors.append({
            "connector_name": name,
            "is_input": is_input,
            "name": name,
            "relation_name": stream
        })

    # Pipeline
    pipeline_name = "demo-supply-chain-tutorial-pipeline"
    create_pipeline(
        api_url,
        {},
        program_name,
        pipeline_name,
        pipeline_description="Supply Chain Tutorial demo pipeline",
        num_workers=8,
        connectors=connectors
    )


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
