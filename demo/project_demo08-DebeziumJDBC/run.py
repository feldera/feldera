# Stream output of a view to Postgres via Debezium JDBC sink connector and Confluent JDBC sink connector.
#
# To run the demo, start RedPanda, Kafka Connect, and Postgres containers:
# > docker compose -f deploy/docker-compose.yml \
#                  -f deploy/docker-compose-extra.yml \
#                  up redpanda kafka-connect postgres --build --renew-anon-volumes --force-recreate
#
# Run this script:
# > python3 run.py --api-url=http://localhost:8080 --start
import os
import time
import datetime
import requests
import argparse
import uuid
from plumbum.cmd import rpk
import psycopg
import random
from feldera import PipelineBuilder, FelderaClient, Pipeline
from typing import Dict


# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")

DATABASE_NAME = "jdbc_test_db"
PIPELINE_NAME = "demo-debezium-jdbc-pipeline"
JSON_CONNECTOR_NAME = "jdbc-test-connector-json"
AVRO_CONNECTOR_NAME = "jdbc-test-connector-avro"
FELDERA_CONNECTOR_NAME = "jdbc-sink"
TABLE_NAME = "test_table"

# The name of the Kafka topic and the resulting Postgres table.
JSON_TABLE_NAME = "json_jdbc_test"
AVRO_TABLE_NAME = "avro_jdbc_test"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--api-url",
        default="http://localhost:8080",
        help="Feldera API URL (e.g., http://localhost:8080 )",
    )
    parser.add_argument(
        "--start", action="store_true", default=False, help="Start the Feldera pipeline"
    )
    parser.add_argument(
        "--kafka-url-from-pipeline",
        required=False,
        default="redpanda:9092",
        help="Kafka URL reachable from the pipeline",
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

    args = parser.parse_args()
    # Delete old connector instance first so it doesn't block topic deletion.
    delete_connector(JSON_CONNECTOR_NAME)
    delete_connector(AVRO_CONNECTOR_NAME)

    # Drop and re-create the database.
    create_database()
    # Create fresh Kafka topics and a new connector instance listening on those topics.

    json_config = {
        "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
        "tasks.max": "1",
        "connection.url": f"jdbc:postgresql://postgres:5432/{DATABASE_NAME}",
        "connection.username": "postgres",
        "connection.password": "postgres",
        "insert.mode": "upsert",
        "primary.key.mode": "record_key",
        "delete.enabled": True,
        "schema.evolution": "basic",
        "database.time_zone": "UTC",
        "dialect.name": "PostgreSqlDatabaseDialect",
        "topics": JSON_TABLE_NAME,
        "errors.deadletterqueue.topic.name": "dlq",
        "errors.deadletterqueue.context.headers.enable": True,
        "errors.deadletterqueue.topic.replication.factor": 1,
        "errors.tolerance": "all",
        "binary.handling.mode": "bytes",
        "decimal.handling.mode": "string",
    }

    create_jdbc_connector(JSON_CONNECTOR_NAME, JSON_TABLE_NAME, json_config)

    avro_config = {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "errors.log.include.messages": True,
        "dialect.name": "PostgreSqlDatabaseDialect",
        "connection.url": f"jdbc:postgresql://postgres:5432/{DATABASE_NAME}",
        "connection.user": "postgres",
        "connection.password": "postgres",
        "insert.mode": "upsert",
        "delete.enabled": True,
        "pk.mode": "record_key",
        "auto.create": True,
        "auto.evolve": True,
        "schema.evolution": "basic",
        "topics": AVRO_TABLE_NAME,
        "errors.deadletterqueue.topic.name": "dlq",
        "errors.deadletterqueue.context.headers.enable": True,
        "errors.deadletterqueue.topic.replication.factor": 1,
        "errors.tolerance": "all",
        "key.converter.key.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicNameStrategy",
        "value.converter.value.subject.name.strategy": "io.confluent.kafka.serializers.subject.TopicNameStrategy",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schemas.enable": True,
        "value.converter.schemas.enable": True,
        "key.converter.schema.registry.url": args.registry_url_from_connect,
        "value.converter.schema.registry.url": args.registry_url_from_connect,
    }

    create_jdbc_connector(AVRO_CONNECTOR_NAME, AVRO_TABLE_NAME, avro_config)

    # Create a pipeline that will write to the topics.
    pipeline = create_feldera_pipeline(
        args.api_url,
        args.kafka_url_from_pipeline,
        args.registry_url_from_pipeline,
        args.start,
    )
    if args.start:
        generate_inputs(pipeline)


def delete_connector(connector_name: str):
    print(f"Deleting old connector {connector_name}")
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")
    # Delete previous connector instance if any.
    requests.delete(f"{connect_server}/connectors/{connector_name}")


def create_database():
    postgres_server = os.getenv("POSTGRES_SERVER", "localhost:6432")
    with psycopg.connect(f"postgresql://postgres:postgres@{postgres_server}") as conn:
        with conn.cursor() as cur:
            conn.autocommit = True
            print(f"(Re-)creating test database {DATABASE_NAME}")
            cur.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
            cur.execute(f"CREATE DATABASE {DATABASE_NAME}")
            print("Database created")


# Wait until the db contains exactly expected_rows rows.
def wait_for_n_outputs(table_name: str, expected_rows: int):
    postgres_server = os.getenv("POSTGRES_SERVER", "localhost:6432")

    with psycopg.connect(
        f"postgresql://postgres:postgres@{postgres_server}/{DATABASE_NAME}"
    ) as conn:
        with conn.cursor() as cur:
            print(f"Waiting for Postgres table {table_name} to be created")
            start_time = time.time()
            while True:
                cur.execute(
                    f"select exists(select * from information_schema.tables where table_name='{table_name}')"
                )
                if cur.fetchone()[0]:
                    print("Done!")
                    break

                print("Table not found")
                if time.time() - start_time >= 100:
                    raise Exception(f"Timeout waiting for {expected_rows} rows")
                else:
                    time.sleep(3)

            print(f"Waiting for {expected_rows} rows in table {table_name}")
            start_time = time.time()
            while True:
                cur.execute(f"SELECT count(id) from {table_name}")
                nrows = cur.fetchone()[0]
                print(f"Found {nrows} rows")
                if nrows == expected_rows:
                    print("Done!")
                    break

                if time.time() - start_time >= 100:
                    raise Exception(f"Timeout waiting for {expected_rows} rows")
                else:
                    time.sleep(3)


def create_jdbc_connector(connector_name: str, topic_name: str, config: Dict):
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")

    # It's important to stop the connector before deleting the topics.
    print("(Re-)creating topic")
    rpk["topic", "delete", topic_name]()
    rpk["topic", "create", topic_name]()

    print("Create connector")
    config = {"name": connector_name, "config": config}

    requests.post(f"{connect_server}/connectors", json=config).raise_for_status()

    print("Checking connector status")
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


def build_sql(pipeline_to_redpanda_server: str, registry_url_from_pipeline: str) -> str:
    return f"""
create table test_table(
    id bigint not null primary key,
    f1 boolean,
    f2 string,
    f3 tinyint,
    f4 decimal(5,2),
    f5 float64,
    f6 time,
    f7 timestamp,
    f8 date,
    f9 binary,
    f10 variant,
    f11 uuid
);

create materialized view test_view
WITH (
    'connectors' = '[
    {{
        "name": "kafka_json",
        "format": {{
            "name": "json",
            "config": {{
                "update_format": "debezium",
                "key_fields": ["id"]
            }}
        }},
        "transport": {{
            "name": "kafka_output",
            "config": {{
                "bootstrap.servers": "{pipeline_to_redpanda_server}",
                "topic": "{JSON_TABLE_NAME}"
            }}
        }}
    }},
    {{
        "name": "kafka_avro",
        "index": "test_view_index",
        "format": {{
            "name": "avro",
            "config": {{
                "update_format": "confluent_jdbc",
                "registry_urls": ["{registry_url_from_pipeline}"]
            }}
        }},
        "transport": {{
            "name": "kafka_output",
            "config": {{
                "bootstrap.servers": "{pipeline_to_redpanda_server}",
                "topic": "{AVRO_TABLE_NAME}"
            }}
        }}
    }}


    ]'
)
as select * from test_table;


create index test_view_index on test_view (id);
    """


def create_feldera_pipeline(
    api_url: str,
    pipeline_to_redpanda_server: str,
    registry_url_from_pipeline: str,
    start_pipeline: bool,
):
    client = FelderaClient(api_url)
    print("Creating the pipeline...")
    sql = build_sql(pipeline_to_redpanda_server, registry_url_from_pipeline)
    pipeline = PipelineBuilder(client, name=PIPELINE_NAME, sql=sql).create_or_replace()

    if start_pipeline:
        print("Starting the pipeline...")
        pipeline.start()
        print("Pipeline started")

    return pipeline


def generate_inputs(pipeline: Pipeline):
    print("Generating records...")
    date_time = datetime.datetime(2024, 1, 30, 8, 58)

    data = []

    for batch in range(0, 100):
        print(f"Batch {batch}")
        for i in range(0, 100):
            data.append(
                {
                    # The number of rows should hit 199 on successful test completion.
                    "id": i + batch,
                    "f1": True,
                    "f2": "foo",
                    "f3": random.randint(0, 100),
                    "f4": "10.5",
                    "f5": 1e-5,
                    "f6": date_time.strftime("%H:%M:%S"),
                    "f7": date_time.strftime("%F %T"),
                    "f8": date_time.strftime("%F"),
                    # "f9": list("bar".encode('utf-8')),
                    "f10": {"id": i + batch, "f1": True, "f2": "foo", "f4": 10.5},
                    "f11": str(uuid.uuid4()),
                }
            )
        inserts = [{"insert": element} for element in data]
        pipeline.input_json("test_table", inserts, update_format="insert_delete")

    wait_for_n_outputs(JSON_TABLE_NAME, 199)
    wait_for_n_outputs(AVRO_TABLE_NAME, 199)

    # deletes = [{"delete": element} for element in data]
    # pipeline.input_json("test_table", deletes, update_format="insert_delete")
    # # #wait_for_n_outputs(JSON_TABLE_NAME, 0)
    # wait_for_n_outputs(AVRO_TABLE_NAME, 0)


if __name__ == "__main__":
    main()
