# Stream output of a view to Postgres via Debesium JDBC sink connector.VIEW_NAME
#
# To run the demo, start Feldera along with RedPanda, Kafka Connect, and Postgres containers:
# > docker compose -f deploy/docker-compose.yml -f deploy/docker-compose-jdbc.yml --profile debezium \
#       up redpanda connect postgres --renew-anon-volumes --force-recreate
#
# Run this script:
# > python3 run.py --api-url=http://localhost:8080 --start
import os
import time
import datetime
import requests
import argparse
from plumbum.cmd import rpk
import psycopg
import random
from feldera import PipelineBuilder, FelderaClient, Pipeline

# File locations
SCRIPT_DIR = os.path.join(os.path.dirname(__file__))
PROJECT_SQL = os.path.join(SCRIPT_DIR, "project.sql")

DATABASE_NAME = "jdbc_test_db"
PIPELINE_NAME = "demo-debezium-jdbc-pipeline"
KAFKA_CONNECT_CONNECTOR_NAME = "jdbc-test-connector"
FELDERA_CONNECTOR_NAME = "jdbc-sink"
TABLE_NAME = "test_table"
# Database view to read from. Also used as the name of the Kafka topic
# to send updates to.
VIEW_NAME = "test_view"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8080", help="Feldera API URL (e.g., http://localhost:8080 )")
    parser.add_argument('--start', action='store_true', default=False, help="Start the Feldera pipeline")
    parser.add_argument('--kafka-url', required=False, default="redpanda:9092", help="Kafka URL reachable from the pipeline")

    args = parser.parse_args()
    # Delete old connector instance first so it doesn't block topic deletion.
    delete_connector()
    # Drop and re-create the database.
    create_database()
    # Create fresh Kafka topics and a new connector instance listening on those topics.
    create_debezium_jdbc_connector()
    # Create a pipeline that will write to the topics.
    pipeline = create_feldera_pipeline(args.api_url, args.kafka_url, args.start)
    if args.start:
        generate_inputs(pipeline)

def delete_connector():
    print("Deleting old connector")
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")
    # Delete previous connector instance if any.
    requests.delete(f"{connect_server}/connectors/{KAFKA_CONNECT_CONNECTOR_NAME}")

def create_database():
    postgres_server = os.getenv("POSTGRES_SERVER", "localhost:6432")
    with psycopg.connect(f"postgresql://postgres:postgres@{postgres_server}") as conn:
        with conn.cursor() as cur:
            conn.autocommit = True
            print(f"(Re-)creating test database {DATABASE_NAME}")
            cur.execute(f"DROP DATABASE IF EXISTS {DATABASE_NAME}")
            cur.execute(f"CREATE DATABASE {DATABASE_NAME}")


# Wait until the db contains exactly expected_rows rows.
def wait_for_n_outputs(expected_rows: int):
    postgres_server = os.getenv("POSTGRES_SERVER", "localhost:6432")

    with psycopg.connect(f"postgresql://postgres:postgres@{postgres_server}/{DATABASE_NAME}") as conn:
        with conn.cursor() as cur:
            print(f"Waiting for Postgres table {VIEW_NAME} to be created")
            start_time = time.time()
            while True:
                cur.execute(f"select exists(select * from information_schema.tables where table_name='{VIEW_NAME}')")
                if cur.fetchone()[0]:
                    print("Done!")
                    break

                print("Table not found")
                if time.time() - start_time >= 100:
                    raise Exception(f"Timeout waiting for {expected_rows} rows")
                else:
                    time.sleep(3)


            print(f"Waiting for {expected_rows} rows in table {VIEW_NAME}")
            start_time = time.time()
            while True:
                cur.execute(f"SELECT count(id) from {VIEW_NAME}")
                nrows = cur.fetchone()[0]
                print(f"Found {nrows} rows")
                if nrows == expected_rows:
                    print("Done!")
                    break

                if time.time() - start_time >= 100:
                    raise Exception(f"Timeout waiting for {expected_rows} rows")
                else:
                    time.sleep(3)


def create_debezium_jdbc_connector():
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")

    # It's important to stop the connector before deleting the topics.
    print(f"(Re-)creating topic")
    rpk["topic", "delete", VIEW_NAME]()
    rpk["topic", "create", VIEW_NAME]()

    print("Create connector")
    config = {
        "name": KAFKA_CONNECT_CONNECTOR_NAME,
        "config": {
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
            "topics": VIEW_NAME,
            "errors.deadletterqueue.topic.name": "dlq",
            "errors.deadletterqueue.context.headers.enable": True,
            "errors.deadletterqueue.topic.replication.factor": 1,
            "errors.tolerance": "all",
            "binary.handling.mode": "bytes",
            "decimal.handling.mode": "string"
        }
    }

    requests.post(
        f"{connect_server}/connectors", json=config
    ).raise_for_status()

    print("Checking connector status")
    start_time = time.time()
    while True:
        response = requests.get(
            f"{connect_server}/connectors/{KAFKA_CONNECT_CONNECTOR_NAME}/status"
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

def build_sql(pipeline_to_redpanda_server: str) -> str:
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
    f9 binary
);

create view test_view
WITH (
    'connectors' = '[{{
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
                "topic": "{VIEW_NAME}"
            }}
        }}
    }}]'
)
as select * from test_table;
    """

def create_feldera_pipeline(api_url: str, pipeline_to_redpanda_server: str, start_pipeline: bool):
    client = FelderaClient(api_url)
    sql = build_sql(pipeline_to_redpanda_server)
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
            data.append({
                    # The number of rows should hit 199 on successful test completion.
                    "id": i + batch,
                    "f1": True,
                    "f2":  "foo",
                    "f3": random.randint(0, 100),
                    "f4": "10.5",
                    "f5": 1e-5,
                    "f6": date_time.strftime("%H:%M:%S"),
                    "f7": date_time.strftime("%F %T"),
                    "f8": date_time.strftime("%F"),
                    #"f9": list("bar".encode('utf-8')),
                })
        inserts = [{"insert": element} for element in data]
        pipeline.input_json("test_table", inserts, update_format="insert_delete")

    wait_for_n_outputs(199)

    deletes = [{"delete": element} for element in data]
    pipeline.input_json("test_table", deletes, update_format="insert_delete")
    wait_for_n_outputs(0)


if __name__ == "__main__":
    main()
