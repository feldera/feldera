# Postgres Debezium source connector demo
#
# Start auxiliary connectors before running this demo
#
# ```bash
# docker compose -f deploy/docker-compose.yml -f deploy/docker-compose-debezium-postgres.yml --profile debezium  up postgres redpanda connect  --force-recreate  -V
# ```

import os
import time
import requests
import argparse
import json
from feldera import FelderaClient, SQLContext
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
    args = parser.parse_args()
    create_debezium_postgres_connector(args.kafka_url_from_script)
    prepare_feldera_pipeline(args.api_url, args.kafka_url_from_pipeline)

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

def prepare_feldera_pipeline(api_url, kafka_url):

    pipeline_name = "demo-debezium-postgres-pipeline"
    client = FelderaClient(api_url)
    pipeline = SQLContext(pipeline_name, client)
    sql = open(PROJECT_SQL).read().replace("[REPLACE-BOOTSTRAP-SERVERS]", kafka_url)

    print("Starting pipeline...")
    pipeline.sql(sql)
    print("Pipeline started")

if __name__ == "__main__":
    main()
