# Postgres Debezium source connector demo.
#
# Creates a Postgres table with a mix of different column types and syncs
# it to identical Feldera tables using JSON and Avro formats.
#
# Start auxiliary containers before running this demo:
#
# ```bash
# docker compose -f deploy/docker-compose.yml \
#                -f deploy/docker-compose-extra.yml \
#                up redpanda kafka-connect postgres --build --renew-anon-volumes --force-recreate
# ```
#
# Example running the script with Feldera running locally (outside of docker compose):
# ```bash
# python3 run.py --kafka-url-from-pipeline=localhost:19092 --registry-url-from-pipeline=http://localhost:18081
# ```

import os
import time
import requests
import argparse
import psycopg
from feldera import FelderaClient, PipelineBuilder
from kafka.admin import KafkaAdminClient
from typing import List, Dict

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")
TEST_SCHEMA = "test_schema"
TEST_TABLE = "test_table"
NUM_RECORDS = 500000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url",
        default="http://localhost:8080",
        help="Feldera API URL (e.g., http://localhost:8080)",
    )
    parser.add_argument(
        "--kafka-url-from-pipeline",
        default="redpanda:9092",
        help="Kafka broker address reachable from the pipeline",
    )
    parser.add_argument(
        "--registry-url-from-pipeline",
        default="http://redpanda:8081",
        help="Schema registry address reachable from the pipeline",
    )
    parser.add_argument(
        "--registry-url-from-connect",
        default="http://redpanda:8081",
        help="Schema registry address reachable from the Kafka Connect server",
    )
    parser.add_argument(
        "--kafka-url-from-script",
        default="localhost:19092",
        help="Kafka broker address reachable from this script",
    )

    args = parser.parse_args()

    populate_database()

    # Create connectors.  The new connectors will continue working with
    # existing Kafka topics created by the previous connectors instance
    # if they exist.

    # Connector using JSON format
    json_config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "postgres",
        "table.include.list": f"{TEST_SCHEMA}.*",
        "topic.prefix": "json",
        "decimal.handling.mode": "string",
        "time.precision.mode": "connect",
    }

    create_debezium_postgres_connector(
        "inventory-connector-json",
        args.kafka_url_from_script,
        json_config,
        [f"json.{TEST_SCHEMA}.{TEST_TABLE}"],
    )

    # Connector using Avro format
    avro_config = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "postgres",
        "table.include.list": f"{TEST_SCHEMA}.*",
        "topic.prefix": "avro",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
        # Every connector needs a separate Postgres replication slot
        "slot.name": "debezium_slot_1",
        "key.converter.schema.registry.url": args.registry_url_from_connect,
        "value.converter.schema.registry.url": args.registry_url_from_connect,
    }

    create_debezium_postgres_connector(
        "test-connector-avro",
        args.kafka_url_from_script,
        avro_config,
        [f"avro.{TEST_SCHEMA}.{TEST_TABLE}"],
    )
    run_feldera_pipeline(
        args.api_url, args.kafka_url_from_pipeline, args.registry_url_from_pipeline
    )


def populate_database():
    postgres_server = os.getenv("POSTGRES_SERVER", "localhost:6432")
    with psycopg.connect(f"postgresql://postgres:postgres@{postgres_server}") as conn:
        with conn.cursor() as cur:
            # conn.autocommit = True
            print(f"(Re-)creating test schema '{TEST_SCHEMA}'")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TEST_SCHEMA}.{TEST_TABLE}(
                        id INT PRIMARY KEY,
                        bi BIGINT,
                        s VARCHAR,
                        d DOUBLE PRECISION,
                        f REAL,
                        i INT,
                        b BOOLEAN,
                        ts TIMESTAMP,
                        dt DATE,
                        json1 JSON,
                        json2 JSON,
                        uuid_ UUID)""")
            cur.execute(f"DELETE FROM {TEST_SCHEMA}.{TEST_TABLE}")

            print(f"Populating '{TEST_SCHEMA}.{TEST_TABLE}'")

            for i in range(NUM_RECORDS):
                cur.execute(f"""
                            INSERT INTO {TEST_SCHEMA}.{TEST_TABLE}
                                    (id, bi, s, d, f, i, b, ts, dt, json1, json2, uuid_)
                            VALUES({i}, {i * 100}, 'foo{i}', {i}.01, {i}.1, {i}, true, '2024-08-30 10:30:00', '2024-08-30', '{{"foo":"bar"}}', '{{"foo":"bar"}}', '123e4567-e89b-12d3-a456-426614174000')
                    """)
                if i % 1000 == 0:
                    print(f"{i} records")


def create_debezium_postgres_connector(
    connector_name: str,
    kafka_url_from_script: str,
    config: Dict,
    expected_topics: List[str],
):
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")

    print(f"Delete old {connector_name} connector")
    # Delete previous connector instance if any.
    # Note: this won't delete any existing Kafka topics created
    # by the connector.
    requests.delete(f"{connect_server}/connectors/{connector_name}")

    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_url_from_script,
        client_id="demo-debezium-postgres",
    )

    print(f"Create {connector_name} connector")
    config = {"name": connector_name, "config": config}

    requests.post(f"{connect_server}/connectors", json=config).raise_for_status()

    print(f"Checking {connector_name} connector status")
    start_time = time.time()
    while True:
        response = requests.get(f"{connect_server}/connectors/{connector_name}/status")
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
    print(f"Waiting for the {connector_name} connector to create Kafka topics")
    start_time = time.time()
    while True:
        topics = set(admin_client.list_topics())
        if all(element in topics for element in expected_topics):
            break

        if time.time() - start_time >= 10:
            raise Exception("Timeout waiting for topic creation")
        print("Waiting for topics")
        time.sleep(1)


def run_feldera_pipeline(api_url, kafka_url, registry_url):
    pipeline_name = "demo-debezium-postgres-pipeline"
    client = FelderaClient(api_url)
    sql = (
        open(PROJECT_SQL)
        .read()
        .replace("[REPLACE-BOOTSTRAP-SERVERS]", kafka_url)
        .replace("[REPLACE-REGISTRY-URL]", registry_url)
    )

    print("Starting pipeline...")
    pipeline = PipelineBuilder(client, name=pipeline_name, sql=sql).create_or_replace()
    pipeline.start()
    print("Pipeline started")


if __name__ == "__main__":
    main()
