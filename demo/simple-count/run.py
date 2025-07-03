#!/bin/python3
import json
import os
import time
import requests
import argparse
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
EXAMPLE_SQL = os.path.join(SCRIPT_DIR, "program.sql")


def main():
    # Command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url",
        required=True,
        help="Feldera API URL (e.g., http://localhost:8080 )",
    )
    parser.add_argument(
        "--kafka",
        default="redpanda:9092",
        required=False,
        help="Kafka bootstrap servers",
    )
    args = parser.parse_args()
    api_url = args.api_url

    # Hostname and port to reach the Kafka server
    script_to_kafka_server = args.kafka
    pipeline_to_kafka_server = args.kafka

    # (Re-)create topics simple_count_input and simple_count_output
    print("(Re-)creating topics...")
    admin_client = KafkaAdminClient(
        bootstrap_servers=script_to_kafka_server,
        client_id="demo-simple-count-admin",
    )
    existing_topics = set(admin_client.list_topics())
    if "simple_count_input" in existing_topics:
        admin_client.delete_topics(["simple_count_input"])
    if "simple_count_output" in existing_topics:
        admin_client.delete_topics(["simple_count_output"])
    admin_client.create_topics(
        [
            NewTopic("simple_count_input", num_partitions=1, replication_factor=1),
            NewTopic("simple_count_output", num_partitions=1, replication_factor=1),
        ]
    )
    print("Topics ready")

    # Produce of rows into the input topic
    print("Producing rows into input topic...")
    num_rows = 1000
    producer = KafkaProducer(
        bootstrap_servers=script_to_kafka_server,
        client_id="demo-simple-count-producer",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    for i in range(num_rows):
        producer.send("simple_count_input", value={"insert": {"id": i}})
    print("Input topic contains data")

    # Test connectivity by fetching the existing pipelines
    print("Checking connectivity by listing pipelines...")
    response = requests.get(f"{api_url}/v0/pipelines?code=false")
    if response.ok:
        print("SUCCESS: can reach the API")
    else:
        print("FAILURE: could not reach API")
        exit(1)

    # Pipeline name
    pipeline_name = "simple-count"

    # Shut down the pipeline if it already exists such that it can be edited
    print("Shutting down the pipeline...")
    if requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").ok:
        requests.post(
            f"{api_url}/v0/pipelines/{pipeline_name}/stop?force=true"
        ).raise_for_status()
        while (
            requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()[
                "deployment_status"
            ]
            != "Stopped"
        ):
            time.sleep(1)

    # Create pipeline
    print("Creating pipeline...")
    program_sql = (
        open(EXAMPLE_SQL)
        .read()
        .replace("[REPLACE-BOOTSTRAP-SERVERS]", pipeline_to_kafka_server)
    )
    response = requests.put(
        f"{api_url}/v0/pipelines/{pipeline_name}",
        json={
            "name": pipeline_name,
            "description": "Description of the simple-count pipeline",
            "runtime_config": {},
            "program_code": program_sql,
            "program_config": {},
        },
    )
    if response.ok:
        print("SUCCESS: created pipeline")
        print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
    else:
        print("FAILURE: could not create pipeline")
        print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
        exit(1)

    # Wait for pipeline program compilation
    check_interval_s = 2  # First is after 2s, then every 5s
    while True:
        response = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}")

        if response.ok:
            pipeline = json.loads(response.content.decode("utf-8"))
            # print(json.dumps(pipeline, indent=4))
            print("Program status: %s" % pipeline["program_status"])
            if pipeline["program_status"] == "Success":
                print("SUCCESS: pipeline program is compiled")
                break
            time.sleep(check_interval_s)
            check_interval_s = 5
        else:
            print("FAILURE: could not check pipeline")
            print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
            exit(1)

    # (Re)start the pipeline
    print("Starting pipeline...")
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start").raise_for_status()
    response = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}")
    resp = response.json()
    deployment_status = resp["deployment_status"]
    deployment_desired_status = resp["deployment_desired_status"]
    while deployment_status != "Running":
        print("Deployment status: %s" % deployment_status)
        if deployment_status == "Stopped" and deployment_desired_status == "Stopped":
            print("FAILED: deployment status is Failed")
            print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
            exit(1)
        response = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}")
        deployment_status = response.json()["deployment_status"]
        time.sleep(1)
    print("Pipeline started")

    # Consume rows from the output topic
    print("Consuming rows from output topic...")
    consumer = KafkaConsumer(
        "simple_count_output",
        bootstrap_servers=script_to_kafka_server,
        auto_offset_reset="earliest",
    )
    for message in consumer:
        finished = False
        for insert_or_delete in json.loads(message.value.decode("utf-8")):
            if "insert" in insert_or_delete:
                if insert_or_delete["insert"]["num_rows"] == num_rows:
                    finished = True
        if finished:
            break
    print(f"All {num_rows} rows were processed")
    print("Finished")


# Main entry point
if __name__ == "__main__":
    main()
