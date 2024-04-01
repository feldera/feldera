# Stream output of a view to Postgres via Debesium JDBC sink connector.VIEW_NAME
#
# To run the demo, start Feldera along with RedPanda, Kafka Connect, and Postgres containers:
# > docker compose -f deploy/docker-compose.yml -f deploy/docker-compose-dev.yml -f deploy/docker-compose-jdbc.yml \
#       up db pipeline-manager redpanda connect postgres --build --renew-anon-volumes --force-recreate
#
# Run this script:
# > python3 run.py --api-url=http://localhost:8080 --start
import base64
import os
import time
import datetime
import requests
import argparse
from plumbum.cmd import rpk
import psycopg
import json
import random

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
PRIMARY_KEY_COLUMN = "id"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", required=True, help="Feldera API URL (e.g., http://localhost:8080 )")
    parser.add_argument('--start', action='store_true', default=False, help="Start the Feldera pipeline")
    args = parser.parse_args()
    # Delete old connector instance first so it doesn't block topic deletion.
    delete_connector()
    # Drop and re-create the database.
    create_database()
    # Create fresh Kafka topics and a new connector instance listening on those topics.
    create_debezium_jdbc_connector()
    # Create a pipeline that will write to the topics.
    prepare_feldera_pipeline(args.api_url, args.start)
    if args.start:
        generate_inputs(args.api_url)

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
            "primary.key.fields": PRIMARY_KEY_COLUMN,
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

def prepare_feldera_pipeline(api_url, start_pipeline):
    pipeline_to_redpanda_server = "redpanda:9092"

    # Create program
    program_name = "demo-debezium-jdbc-program"
    program_sql = open(PROJECT_SQL).read()
    response = requests.put(f"{api_url}/v0/programs/{program_name}", json={
        "description": "Debezium JDBC sink connector demo program",
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
    for (connector_name, stream, topic) in [
        (FELDERA_CONNECTOR_NAME, VIEW_NAME,  VIEW_NAME),
    ]:
        requests.put(f"{api_url}/v0/connectors/{connector_name}", json={
            "description": "",
            "config": {
                "format": {
                    "name": "json",
                    "config": {
                        "update_format": "debezium",
                    }
                },
                "transport": {
                    "name": "kafka_output",
                    "config": {
                        "bootstrap.servers": pipeline_to_redpanda_server,
                        "topic": topic
                    }
                }
            }
        })
        connectors.append({
            "connector_name": connector_name,
            "is_input": False,
            "name": connector_name,
            "relation_name": stream
        })

    # Create pipeline
    requests.put(f"{api_url}/v0/pipelines/{PIPELINE_NAME}", json={
        "description": "",
        "config": {
            "workers": 8,
            # Don't start computing until we have at least 1M input records or after 10 seconds.
            #"min_batch_size_records": 1000000,
            #"max_buffering_delay_usecs": 10000000,
        },
        "program_name": program_name,
        "connectors": connectors,
    }).raise_for_status()

    # Start pipeline
    if start_pipeline:
        print("(Re)starting pipeline...")
        requests.post(f"{api_url}/v0/pipelines/{PIPELINE_NAME}/shutdown").raise_for_status()
        while requests.get(f"{api_url}/v0/pipelines/{PIPELINE_NAME}").json()["state"]["current_status"] != "Shutdown":
            time.sleep(1)
        requests.post(f"{api_url}/v0/pipelines/{PIPELINE_NAME}/start").raise_for_status()
        while requests.get(f"{api_url}/v0/pipelines/{PIPELINE_NAME}").json()["state"]["current_status"] != "Running":
            time.sleep(1)
        print("Pipeline (re)started")

def generate_inputs(api_url):
    print("Generating records...")
    date_time = datetime.datetime(2024, 1, 30, 8, 58)
    
    for batch in range(0, 100):
        print(f"Batch {batch}")
        data = ""
        for i in range(0, 100):
            data += json.dumps({
                "insert": {
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
                }
            }) + "\n"
        requests.post(f'{api_url}/v0/pipelines/{PIPELINE_NAME}/ingress/{TABLE_NAME}?format=json', data=data).raise_for_status()

    wait_for_n_outputs(199)

if __name__ == "__main__":
    main()
