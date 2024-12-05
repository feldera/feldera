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
    rpk["topic", "delete", "fraud_demo_large_demographics"]()
    rpk["topic", "delete", "fraud_demo_large_transactions"]()
    rpk["topic", "delete", "fraud_demo_large_enriched"]()
    rpk[
        "topic",
        "create",
        "fraud_demo_large_demographics",
        "-c",
        "retention.ms=-1",
        "-c",
        "retention.bytes=-1",
    ]()
    rpk[
        "topic",
        "create",
        "fraud_demo_large_transactions",
        "-c",
        "retention.ms=-1",
        "-c",
        "retention.bytes=-1",
    ]()
    rpk["topic", "create", "fraud_demo_large_enriched"]()
    print("(Re-)created Kafka topics")

    transactions_csv = os.path.join(SCRIPT_DIR, "transactions.csv")
    demographics_csv = os.path.join(SCRIPT_DIR, "demographics.csv")

    if not os.path.exists(transactions_csv):
        from plumbum.cmd import gdown

        print("Downloading transactions.csv (~2 GiB)...")
        gdown["1YuiKl-MMbEujTOwPOyxEoVCh088y9jxI", "--output", transactions_csv]()

    # Push test data to topics
    print("Pushing demographics data to Kafka topic...")
    with open(demographics_csv, "r") as f:
        for n_lines in iter(lambda: tuple(islice(f, 1000)), ()):
            (
                rpk["topic", "produce", "fraud_demo_large_demographics", "-f", "%v"]
                << "\n".join(n_lines)
            )()
    print("Pushing transaction data to Kafka topic...")
    with open(transactions_csv, "r") as f:
        for n_lines in iter(lambda: tuple(islice(f, 8_000)), ()):
            (
                rpk["topic", "produce", "fraud_demo_large_transactions", "-f", "%v"]
                << "\n".join(n_lines)
            )()


def prepare_feldera(api_url, start_pipeline):
    pipeline_to_redpanda_server = "redpanda:9092"

    # Create program
    program_name = "demo-time-series-enrich-program"
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
        (
            "time-series-enrich-demographics-large",
            "DEMOGRAPHICS",
            ["fraud_demo_large_demographics"],
            True,
        ),
        (
            "time-series-enrich-transactions-large",
            "TRANSACTIONS",
            ["fraud_demo_large_transactions"],
            True,
        ),
        (
            "time-series-enrich-join-demographics-with-transactions",
            "TRANSACTIONS_WITH_DEMOGRAPHICS",
            "fraud_demo_large_enriched",
            False,
        ),
    ]:
        # Create connector
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
    pipeline_name = "demo-time-series-enrich-pipeline"
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
