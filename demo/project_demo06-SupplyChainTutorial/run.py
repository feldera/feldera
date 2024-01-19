import os
import time
import requests
import argparse
from plumbum.cmd import rpk

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")


def main():
    parser = argparse.ArgumentParser(
        description="Demo tutorial combining supply chain concepts (e.g., price, vendor, part) and "
                    "generating insights (e.g, lowest price, preferred vendor)"
    )
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    parser.add_argument('--start', action='store_true', default=False, help="Start the Feldera pipeline")
    args = parser.parse_args()
    api_url = args.api_url
    start_pipeline = args.start
    pipeline_to_redpanda_server = "redpanda:9092"

    # Kafka topics
    print("(Re-)creating topics price and preferred_vendor...")
    rpk["topic", "delete", "price"]()
    rpk["topic", "create", "price"]()
    rpk["topic", "delete", "preferred_vendor"]()
    rpk["topic", "create", "preferred_vendor"]()
    print("(Re-)created topics price and preferred_vendor")

    # Create program
    program_name = "demo-supply-chain-tutorial-program"
    program_sql = open(PROJECT_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{program_name}", json={
        "description": "Supply Chain Tutorial demo program",
        "code": program_sql
    })
    response.raise_for_status()
    program_version = response.json()["version"]

    # Compile program
    print(f"Compiling program {program_name} (version: {program_version})...")
    requests.post(f"{api_url}/v0/programs/{program_name}/compile", json={"version": program_version}).raise_for_status()
    while True:
        status = requests.get(f"{api_url}/v0/programs/{program_name}").json()["status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(5)

    # Connectors
    connectors = []
    for (connector_name, stream, config, is_input) in [
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
        requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
            "description": "",
            "config": config
        })
        connectors.append({
            "connector_name": connector_name,
            "is_input": is_input,
            "name": connector_name,
            "relation_name": stream
        })

    # Create pipeline
    pipeline_name = "demo-supply-chain-tutorial-pipeline"
    requests.put(f"{api_url}/v0/pipelines/{pipeline_name}", json={
        "description": "Supply Chain Tutorial demo pipeline",
        "config": {"workers": 8},
        "program_name": program_name,
        "connectors": connectors,
    }).raise_for_status()

    # Start pipeline
    if start_pipeline:
        print("(Re)starting pipeline...")
        requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
        while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Shutdown":
            time.sleep(1)
        requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start").raise_for_status()
        while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Running":
            time.sleep(1)
        print("Pipeline (re)started")


if __name__ == "__main__":
    main()
