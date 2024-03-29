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
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    api_url = parser.parse_args().api_url

    # Hostname and port to reach the Kafka server
    script_to_kafka_server = "redpanda:9092"
    pipeline_to_kafka_server = "redpanda:9092"

    # Test connectivity by fetching the existing programs
    requests.get(f"{api_url}/v0/programs").raise_for_status()

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
    admin_client.create_topics([
        NewTopic("simple_count_input", num_partitions=1, replication_factor=1),
        NewTopic("simple_count_output", num_partitions=1, replication_factor=1),
    ])
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

    # Create a service
    print("Creating service simple-count-kafka-service...")
    requests.put(f"{api_url}/v0/services/simple-count-kafka-service", json={
        "description": "Kafka service for the simple-count demo",
        "config": {
            "kafka": {
                "bootstrap_servers": [pipeline_to_kafka_server],
                "options": {}
            }
        }
    }).raise_for_status()
    print("Service created")

    # Create program
    print("Creating program...")
    program_name = "demo-simple-count-program"
    program_sql = open(EXAMPLE_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{program_name}", json={
        "description": "",
        "code": program_sql
    })
    response.raise_for_status()
    program_version = response.json()["version"]
    print("Program created")

    # Connectors
    print("Creating connectors...")
    connectors = []
    for (connector_name, stream, topic_or_topics, is_input) in [
        ("simple-count-example-input", 'example',  ["simple_count_input"], True),
        ("simple-count-example-output", 'example_count', "simple_count_output", False),
    ]:
        if is_input:
            response = requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
                "description": "",
                "config": {
                    "transport": {
                        "name": "kafka_input",
                        "config": {
                            "topics": topic_or_topics,
                            "kafka_service": "simple-count-kafka-service",
                            "auto.offset.reset": "earliest",
                        }
                    },
                    "format": {
                        "name": "json",
                        "config": {
                            "update_format": "insert_delete",
                            "array": False
                        }
                    }
                }
            })
            if not response.ok:
                print(response.content)
                response.raise_for_status()
        else:
            response = requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
                "description": "",
                "config": {
                    "transport": {
                        "name": "kafka_output",
                        "config": {
                            "topic": topic_or_topics,
                            "kafka_service": "simple-count-kafka-service",
                        }
                    },
                    "format": {
                        "name": "json",
                        "config": {
                            "update_format": "insert_delete",
                            "array": True
                        }
                    }
                }
            })
            if not response.ok:
                print(response.content)
                response.raise_for_status()
        connectors.append({
            "connector_name": connector_name,
            "is_input": is_input,
            "name": connector_name,
            "relation_name": stream
        })
    print("Connectors created")

    # Create pipeline
    print("Creating pipeline...")
    pipeline_name = "demo-simple-count-pipeline"
    requests.put(f"{api_url}/v0/pipelines/{pipeline_name}", json={
        "description": "",
        "config": {"workers": 8},
        "program_name": program_name,
        "connectors": connectors,
    }).raise_for_status()
    print("Pipeline created")

    # Compile program
    print(f"Compiling program {program_name} (version: {program_version})...")
    while True:
        status = requests.get(f"{api_url}/v0/programs/{program_name}").json()["status"]
        print(f"Program status: {status}")
        if status == "Success":
            break
        elif status != "Pending" and status != "CompilingRust" and status != "CompilingSql":
            raise RuntimeError(f"Failed program compilation with status {status}")
        time.sleep(5)
    print("Program compiled")

    # Start pipeline
    print("(Re)starting pipeline...")
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/shutdown").raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Shutdown":
        time.sleep(1)
    requests.post(f"{api_url}/v0/pipelines/{pipeline_name}/start").raise_for_status()
    while requests.get(f"{api_url}/v0/pipelines/{pipeline_name}").json()["state"]["current_status"] != "Running":
        time.sleep(1)
    print("Pipeline (re)started")

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
