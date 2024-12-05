import os
import time
import requests
import argparse
from itertools import islice
from plumbum.cmd import rpk

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url",
        required=True,
        help="Feldera API URL (e.g., http://localhost:8080 )",
    )
    parser.add_argument(
        "--start", action="store_true", default=False, help="Start the Feldera pipeline"
    )
    args = parser.parse_args()
    prepare_redpanda()
    prepare_feldera(args.api_url, args.start)


def prepare_redpanda():
    # Prepare Kafka topics
    print("(Re-)creating Kafka topics...")
    rpk["topic", "delete", "green_trip_demo_large_input"]()
    rpk["topic", "delete", "green_trip_demo_large_output"]()
    rpk[
        "topic",
        "create",
        "green_trip_demo_large_input",
        "-c",
        "retention.ms=-1",
        "-c",
        "retention.bytes=-1",
    ]()
    rpk["topic", "create", "green_trip_demo_large_output"]()
    print("(Re-)created Kafka topics")

    green_tripdata_csv = os.path.join(SCRIPT_DIR, "green_tripdata.csv")

    if not os.path.exists(green_tripdata_csv):
        print("Downloading green_tripdata.csv...")
        from plumbum.cmd import gdown

        gdown["1R1LdIRDlvN50cq1nMzhRWl8hw-rjCbr6", "--output", green_tripdata_csv]()

    # Push test data to topics
    print("Pushing tripdata data to Kafka topic...")
    with open(green_tripdata_csv, "r") as f:
        for n_lines in iter(lambda: tuple(islice(f, 10_000)), ()):
            (
                rpk["topic", "produce", "green_trip_demo_large_input", "-f", "%v"]
                << "\n".join(n_lines)
            )()


def prepare_feldera(api_url, start_pipeline):
    pipeline_to_redpanda_server = "redpanda:9092"

    # Create program
    program_name = "demo-green-trip-program"
    program_sql = open(PROJECT_SQL).read()
    response = requests.put(
        f"{api_url}/v0/programs/{program_name}",
        json={"description": "", "code": program_sql},
    )
    response.raise_for_status()
    program_version = response.json()["version"]

    # Compile program
    print(f"Compiling program {program_name} (version: {program_version})...")
    requests.post(
        f"{api_url}/v0/programs/{program_name}/compile",
        json={"version": program_version},
    ).raise_for_status()
    while True:
        status = requests.get(f"{api_url}/v0/programs/{program_name}").json()["status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif (
            status != "Pending"
            and status != "CompilingRust"
            and status != "SqlCompiled"
            and status != "CompilingSql"
        ):
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(5)

    # Connectors
    connectors = []
    for connector_name, stream, topic_topics, is_input in [
        ("green-trips-data", "GREEN_TRIPDATA", ["green_trip_demo_large_input"], True),
        (
            "green-trips-online-feature-calculation",
            "FEATURES",
            "green_trip_demo_large_output",
            False,
        ),
    ]:
        requests.put(
            f"{api_url}/v0/connectors/{connector_name}",
            json={
                "description": "",
                "config": {
                    "format": {"name": "csv", "config": {}},
                    "transport": {
                        "name": "kafka_" + ("input" if is_input else "output"),
                        "config": {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topic": topic_topics,
                        }
                        if not is_input
                        else {
                            "bootstrap.servers": pipeline_to_redpanda_server,
                            "topics": topic_topics,
                            "auto.offset.reset": "earliest",
                        },
                    },
                },
            },
        )
        connectors.append(
            {
                "connector_name": connector_name,
                "is_input": is_input,
                "name": connector_name,
                "relation_name": stream,
            }
        )

    # Create pipeline
    pipeline_name = "demo-green-trip-pipeline"
    requests.put(
        f"{api_url}/v0/pipelines/{pipeline_name}",
        json={
            "description": "",
            "config": {"workers": 8},
            "program_name": program_name,
            "connectors": connectors,
        },
    ).raise_for_status()

    # Start pipeline
    if start_pipeline:
        print("(Re)starting pipeline...")
        requests.post(
            f"{api_url}/v0/pipelines/{pipeline_name}/shutdown"
        ).raise_for_status()
        while (
            requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"][
                "current_status"
            ]
            != "Shutdown"
        ):
            time.sleep(1)
        requests.post(
            f"{api_url}/v0/pipelines/{pipeline_name}/start"
        ).raise_for_status()
        while (
            requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"][
                "current_status"
            ]
            != "Running"
        ):
            time.sleep(1)
        print("Pipeline (re)started")


if __name__ == "__main__":
    main()
