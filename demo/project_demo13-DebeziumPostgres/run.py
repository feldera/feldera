import os
import time
import requests
import argparse
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")

expected_topics = [ "inventory.inventory.orders",
           "inventory.inventory.geom",
           "inventory.inventory.customers",
           "inventory.inventory.products",
           "inventory.inventory.products_on_hand",
         ]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    parser.add_argument("--kafka-url-from-pipeline", default="redpanda:9092", help="Kafka broker address reachable from the pipeline")
    parser.add_argument("--kafka-url-from-script", default="localhost:19092", help="Kafka broker address reachable from this script")
    parser.add_argument('--start', action='store_true', default=False, help="Start the Feldera pipeline")
    args = parser.parse_args()
    create_debezium_postgres_connector(args.kafka_url_from_script)
    prepare_feldera_pipeline(args.api_url, args.kafka_url_from_pipeline, args.start)


def create_debezium_postgres_connector(kafka_url_from_script):
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")

    print("Delete old connector")
    # Delete previous connector instance if any.
    # Note: this won't delete any existing Kafka topics created
    # by the connector.
    requests.delete(f"{connect_server}/connectors/inventory-connector")

    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_url_from_script,
        client_id="demo-debezium-postgres",
    )

    print("Create connector")
    # Create connector.  The new connector will continue working with
    # existing Kafka topics created by the previous connectors instance
    # if they exist.
    config = {
        "name": "inventory-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "postgres",
            "database.port": "5432",
            "database.user": "postgres",
            "database.password": "postgres",
            "database.dbname": "postgres",
            "table.include.list": "inventory.*",
            "topic.prefix": "inventory",
            "decimal.handling.mode": "string",
            "time.precision.mode": "connect"
        },
    }
    requests.post(
        f"{connect_server}/connectors", json=config
    ).raise_for_status()

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
        topics = set(admin_client.list_topics())
        if all(element in topics for element in expected_topics):
            break

        if time.time() - start_time >= 10:
            raise Exception("Timeout waiting for topic creation")
        print("Waiting for topics")
        time.sleep(1)

def prepare_feldera_pipeline(api_url, kafka_url, start_pipeline):

    # Create program
    program_name = "demo-debezium-postgres-program"
    program_sql = open(PROJECT_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{program_name}", json={
        "description": "Simple Select Program",
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
    for (connector_name, stream, topics) in [
        ("customers", 'CUSTOMERS', ["inventory.inventory.customers"]),
        ("orders", 'ORDERS', ["inventory.inventory.orders"]),
        ("products", 'PRODUCTS', ["inventory.inventory.products"]),
        ("products_on_hand", 'PRODUCTS_ON_HAND', ["inventory.inventory.products_on_hand"]),
    ]:
        requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
            "description": "",
            "config": {
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "debezium",
                        "json_flavor": "debezium_postgres"
                    }
                },
                "transport": {
                    "name": "kafka_input",
                    "config": {
                        "bootstrap.servers": kafka_url,
                        "auto.offset.reset": "earliest",
                        "topics": topics
                    }
                }
            }
        })
        connectors.append({
            "connector_name": connector_name,
            "is_input": True,
            "name": connector_name,
            "relation_name": stream
        })

    # Create pipeline
    pipeline_name = "demo-debezium-postgres-pipeline"
    requests.put(f"{api_url}/v0/pipelines/{pipeline_name}", json={
        "description": "",
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
