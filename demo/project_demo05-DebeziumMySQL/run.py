import os
import sys
import time
import requests

from dbsp import DBSPPipelineConfig
from dbsp import JsonInputFormatConfig, JsonOutputFormatConfig
from dbsp import KafkaInputConfig
from dbsp import KafkaOutputConfig

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".."))
from demo import *

SCRIPT_DIR = os.path.join(os.path.dirname(__file__))


def prepare(args=[]):
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")

    print("Delete old connector")
    # Delete previous connector instance if any.
    # Note: this won't delete any existing Kafka topics created
    # by the connector.
    requests.delete(f"{connect_server}/connectors/inventory-connector")

    print("Create connector")
    # Create connector.  The new connector will continue working with
    # existing Kafka topics created by the previous connectors instance
    # if the exist.
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
        },
    }
    response = requests.post(
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
                break
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
                break
            print("Waiting for topics")
            time.sleep(1)


def make_config(project):
    config = DBSPPipelineConfig(project, 8, "Debezium MySQL Pipeline")

    config.add_kafka_input(
        name="addresses",
        stream="ADDRESSES",
        config=KafkaInputConfig.from_dict(
            {
                "topics": ["inventory.inventory.addresses"],
                "auto.offset.reset": "earliest",
            }
        ),
        format=JsonInputFormatConfig(update_format="debezium"),
    )

    config.add_kafka_input(
        name="customers",
        stream="CUSTOMERS",
        config=KafkaInputConfig.from_dict(
            {
                "topics": ["inventory.inventory.customers"],
                "auto.offset.reset": "earliest",
            }
        ),
        format=JsonInputFormatConfig(update_format="debezium"),
    )

    config.add_kafka_input(
        name="orders",
        stream="ORDERS",
        config=KafkaInputConfig.from_dict(
            {
                "topics": ["inventory.inventory.orders"],
                "auto.offset.reset": "earliest",
            }
        ),
        format=JsonInputFormatConfig(update_format="debezium"),
    )

    config.add_kafka_input(
        name="products",
        stream="PRODUCTS",
        config=KafkaInputConfig.from_dict(
            {
                "topics": ["inventory.inventory.products"],
                "auto.offset.reset": "earliest",
            }
        ),
        format=JsonInputFormatConfig(update_format="debezium"),
    )

    config.add_kafka_input(
        name="products_on_hand",
        stream="PRODUCTS_ON_HAND",
        config=KafkaInputConfig.from_dict(
            {
                "topics": ["inventory.inventory.products_on_hand"],
                "auto.offset.reset": "earliest",
            }
        ),
        format=JsonInputFormatConfig(update_format="debezium"),
    )

    config.save()
    return config


if __name__ == "__main__":
    run_demo(
        "Debezium MySQL Demo",
        os.path.join(SCRIPT_DIR, "project.sql"),
        make_config,
        prepare,
    )
