import os
import time
import json
import requests
import argparse

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
    create_debezium_mysql_connector()
    prepare_feldera_pipeline(args.api_url, args.start)


def create_debezium_mysql_connector():
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")

    print("Delete old connector")
    # Delete previous connector instance if any.
    # Note: this won't delete any existing Kafka topics created
    # by the connector.
    requests.delete(f"{connect_server}/connectors/inventory-connector")

    print("Create connector")
    # Create connector.  The new connector will continue working with
    # existing Kafka topics created by the previous connectors instance
    # if they exist.
    config = {
        "name": "inventory-connector",
        "config": {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "tasks.max": "1",
            "database.hostname": "mysql",
            "database.port": "3306",
            "database.user": "debezium",
            "database.password": "dbz",
            "database.server.id": "184054",
            "database.server.name": "dbserver1",
            "database.include.list": "inventory",
            "database.history.kafka.bootstrap.servers": "redpanda:9092",
            "topic.prefix": "inventory",
            "schema.history.internal.kafka.topic": "schema-changes.inventory.internal",
            "schema.history.internal.kafka.bootstrap.servers": "redpanda:9092",
            "include.schema.changes": "true",
            "decimal.handling.mode": "string",
        },
    }
    requests.post(f"{connect_server}/connectors", json=config).raise_for_status()

    print("Checking connector status")
    start_time = time.time()
    while True:
        response = requests.get(
            f"{connect_server}/connectors/inventory-connector/status"
        )
        print(f"response: {response}")
        if response.ok:
            status = response.json()
            print(f"status: {status}")
            if status["connector"]["state"] != "RUNNING":
                raise Exception(f"Unexpected connector state: {status}")
            if len(status["tasks"]) == 0:
                print("Waiting for connector task")
                time.sleep(1)
                continue
            if status["tasks"][0]["state"] != "RUNNING":
                raise Exception(f"Unexpected task state: {status}")
            break
        else:
            if time.time() - start_time >= 5:
                raise Exception("Timeout waiting for connector creation")
            print("Waiting for connector creation")
            time.sleep(1)

    # Connector is up, but this doesn't guarantee that it has created all 5 topics.
    print("Waiting for the connector to create Kafka topics")
    start_time = time.time()
    while True:
        response = requests.get(
            f"{connect_server}/connectors/inventory-connector/topics"
        )
        if not response.ok:
            raise Exception(f"Error retrieving topic list from connector: {response}")
        else:
            status = response.json()
            print(f"topics: {status}")
            topics = status["inventory-connector"]["topics"]
            if all(
                element in topics
                for element in [
                    "inventory.inventory.orders",
                    "inventory.inventory.addresses",
                    "inventory.inventory.customers",
                    "inventory.inventory.products",
                    "inventory.inventory.products_on_hand",
                ]
            ):
                break

            if time.time() - start_time >= 10:
                raise Exception("Timeout waiting for topic creation")
            print("Waiting for topics")
            time.sleep(1)


def prepare_feldera_pipeline(api_url, start_pipeline):
    pipeline_to_kafka_server = "redpanda:9092"

    # Test connectivity by fetching the existing pipelines
    print("Checking connectivity by listing pipelines...")
    response = requests.get(f"{api_url}/v0/pipelines?code=false")
    if response.ok:
        print("SUCCESS: can reach the API")
    else:
        print("FAILURE: could not reach API")
        exit(1)

    # Pipeline name
    pipeline_name = "debezium-mysql"

    # Stop the pipeline if it already exists such that it can be edited
    print("Stopping the pipeline...")
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
        open(PROJECT_SQL)
        .read()
        .replace("[REPLACE-BOOTSTRAP-SERVERS]", pipeline_to_kafka_server)
    )
    response = requests.put(
        f"{api_url}/v0/pipelines/{pipeline_name}",
        json={
            "name": pipeline_name,
            "description": "Description of the debezium-mysql pipeline",
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
    while True:
        response = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}")
        if response.ok:
            pipeline = json.loads(response.content.decode("utf-8"))
            print("Program status: %s" % pipeline["program_status"])
            if pipeline["program_status"] == "Success":
                break
            time.sleep(5)
        else:
            print("FAILURE: could not check pipeline")
            print(json.dumps(json.loads(response.content.decode("utf-8")), indent=4))
            exit(1)

    # (Re)start the pipeline
    if start_pipeline:
        print("Starting pipeline...")
        requests.post(
            f"{api_url}/v0/pipelines/{pipeline_name}/start"
        ).raise_for_status()
        response = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}")
        resp = response.json()
        deployment_status = resp["deployment_status"]
        deployment_desired_status = resp["deployment_desired_status"]
        while deployment_status != "Running":
            print("Deployment status: %s" % deployment_status)
            if (
                deployment_status == "Stopped"
                and deployment_desired_status == "Stopped"
            ):
                print("FAILED: deployment status is Failed")
                print(
                    json.dumps(json.loads(response.content.decode("utf-8")), indent=4)
                )
                exit(1)
            response = requests.get(f"{api_url}/v0/pipelines/{pipeline_name}")
            deployment_status = response.json()["deployment_status"]
            time.sleep(1)
        print("Pipeline started")


if __name__ == "__main__":
    main()
