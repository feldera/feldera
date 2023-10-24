import os
import subprocess
import sys
import time
import requests
import uuid
import snowflake.connector

from dbsp import DBSPPipelineConfig
from dbsp import JsonOutputFormatConfig
from dbsp import KafkaOutputConfig

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(__file__)), ".."))
from demo import *

SCRIPT_DIR = os.path.join(os.path.dirname(__file__))

SNOWFLAKE_CI_ACCOUNT_NAME = "JBHMQPR-WMB83241"
SNOWFLAKE_CI_DATABASE = "CI"
SNOWFLAKE_CI_USER_NAME = "CI_1"
SCHEMA_UUID = uuid.uuid4()
SCHEMA_NAME = f"supply_chain_demo_{str(SCHEMA_UUID).replace('-', '_')}"
LANDING_SCHEMA_NAME = f"{SCHEMA_NAME}_landing"

SNOWFLAKE_CI_USER_PASSWORD = os.getenv("SNOWFLAKE_CI_USER_PASSWORD")
if SNOWFLAKE_CI_USER_PASSWORD is None:
    raise EnvironmentError(
        f"The environment variable SNOWFLAKE_CI_USER_PASSWORD is not defined."
    )


def prepare(args=[]):
    connect_server = os.getenv("KAFKA_CONNECT_SERVER", "http://localhost:8083")

    passphrase = os.getenv("SNOWFLAKE_CI_USER_PRIVATE_KEY_PASSPHRASE")
    if passphrase is None:
        raise EnvironmentError(
            f"The environment variable SNOWFLAKE_CI_USER_PRIVATE_KEY_PASSPHRASE is not defined."
        )
    private_key = os.getenv("SNOWFLAKE_CI_USER_PRIVATE_KEY")
    if private_key is None:
        raise EnvironmentError(
            f"The environment variable SNOWFLAKE_CI_USER_PRIVATE_KEY is not defined."
        )

    # Run snowsql script to create landing and target tables in Snowflake.
    #
    # TODO: Ideally we should do this using the Snowlake Python API, but
    # at least for now I want to have it in a SnowSQL script (makes it easier
    # to experiment), and I don't think it's possible to execute that script
    # via the API because it uses SnowSQL-specific commands (lines that start
    # with `!`)
    cmd = [
        "snowsql",
        "--accountname",
        SNOWFLAKE_CI_ACCOUNT_NAME,
        "--username",
        SNOWFLAKE_CI_USER_NAME,
        "-f",
        "setup.sql",
        "-D",
        f"schema_name={SCHEMA_NAME}",
    ]

    env = os.environ.copy()
    env["SNOWSQL_PWD"] = SNOWFLAKE_CI_USER_PASSWORD
    subprocess.run(
        cmd,
        cwd=SCRIPT_DIR,
        check=True,
        env=env,
    )

    print("Delete old connector")
    # Delete previous connector instance if any.
    # Note: this won't delete any existing Kafka topics created
    # by the connector.
    requests.delete(f"{connect_server}/connectors/snowflake-demo")

    print("Create connector")

    # Create connector.  The new connector will continue working with
    # existing Kafka topics created by the previous connectors instance.
    config = {
        "name": "snowflake-demo",
        "config": {
            "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
            "tasks.max": "8",
            "topics": "snowflake.price,snowflake.preferred_vendor",
            # Map Kafka topics to Snowflake tables within the database and schema specified below.
            "snowflake.topic2table.map": "snowflake.price:price,snowflake.preferred_vendor:preferred_vendor",
            # Don't go to FAILED state on errors, write faulty records to the DLQ topic.
            "errors.tolerance": "all",
            # Topic for rejected Kafka messages.
            "errors.deadletterqueue.topic.name": "snowflake-test-dlq",
            "errors.deadletterqueue.topic.replication.factor": "1",
            # Tells the connector to output additional metadata about the rejected message to the DLQ topic.
            "errors.deadletterqueue.context.headers.enable": "true",
            # Use Snowpipe streaming ingest instead of regular Snowpipe ingest using staging files.
            "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
            # Enable schema evolition to parse JSON values into strongly typed columns instead
            # of storing raw JSON in staging tables.
            "snowflake.enable.schematization": "TRUE",
            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "snowflake.url.name": "JBHMQPR-WMB83241.snowflakecomputing.com:443",
            # Authenticate as `ci_1` user.
            "snowflake.user.name": "ci_1",
            "snowflake.role.name": "ci_role_1",
            "snowflake.private.key": private_key,
            "snowflake.private.key.passphrase": passphrase,
            # Dedicated CI database where we keep all our test tables.
            "snowflake.database.name": "ci",
            # The `setup.sql` script creates two schemas: `<schema_name>` for target tables
            # and `<schema_name>_landing` for landing tables where the data will be written
            # before getting ingested in the target tables.
            "snowflake.schema.name": f"{SCHEMA_NAME}_landing",
            # Flush data to Snowflake every second.
            "buffer.flush.time": "1",
            # Additionally, we need to set the Kafka poll interval:
            # https://github.com/snowflakedb/snowflake-kafka-connector/issues/721
            "max.poll.interval.ms": "10000",
            "buffer.count.records": "10000",
        },
    }

    print(f"connector config: {config}")

    response = requests.post(
        f"{connect_server}/connectors", json=config
    ).raise_for_status()

    print("Checking connector status")
    start_time = time.time()
    while True:
        response = requests.get(f"{connect_server}/connectors/snowflake-demo/status")
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


def make_config(project):
    config = DBSPPipelineConfig(project, 8, "Snowflake Demo Pipeline")

    config.add_kafka_output(
        name="price",
        stream="PRICE_OUT",
        config=KafkaOutputConfig.from_dict(
            {
                "topic": "snowflake.price",
            }
        ),
        format=JsonOutputFormatConfig(update_format="snowflake"),
    )

    config.add_kafka_output(
        name="preferred_vendor",
        stream="PREFERRED_VENDOR",
        config=KafkaOutputConfig.from_dict(
            {
                "topic": "snowflake.preferred_vendor",
            }
        ),
        format=JsonOutputFormatConfig(update_format="snowflake"),
    )

    config.save()
    return config


def verify(dbsp_url, pipeline: DBSPPipelineConfig):
    print("Pushing PART data")
    requests.post(
        f"{dbsp_url}/v0/pipelines/{pipeline.pipeline_id}/ingress/PART?format=json",
        data=r"""{"insert": {"id": 1, "name": "Flux Capacitor"}}
{"insert": {"id": 2, "name": "Warp Core"}}
{"insert": {"id": 3, "name": "Kyber Crystal"}}""",
    ).raise_for_status()

    print("Pushing VENDOR data")
    requests.post(
        f"{dbsp_url}/v0/pipelines/{pipeline.pipeline_id}/ingress/VENDOR?format=json",
        data=r"""{"insert": {"id": 1, "name": "Gravitech Dynamics", "address": "222 Graviton Lane"}}
{"insert": {"id": 2, "name": "HyperDrive Innovations", "address": "456 Warp Way"}}
{"insert": {"id": 3, "name": "DarkMatter Devices", "address": "333 Singularity Street"}}""",
    ).raise_for_status()

    print("Pushing PRICE data")
    requests.post(
        f"{dbsp_url}/v0/pipelines/{pipeline.pipeline_id}/ingress/PRICE?format=json",
        data=r"""{"insert": {"part": 1, "vendor": 2, "created": "2019-05-20 13:37:03", "effective_since": "2019-05-21", "price": 10000, "f": 0.123}}
{"insert": {"part": 2, "vendor": 1, "created": "2023-10-9 00:00:00", "effective_since": "2023-10-10", "price": 15000, "f": 12345E-2}}
{"insert": {"part": 3, "vendor": 3, "created": "2024-01-01 11:15:00", "effective_since": "2024-01-01", "price": 9000, "f": 12345}}""",
    ).raise_for_status()

    print("Connecting to Snowflake")
    # Connect to the DB.
    connection = snowflake.connector.connect(
        account=SNOWFLAKE_CI_ACCOUNT_NAME,
        user=SNOWFLAKE_CI_USER_NAME,
        password=SNOWFLAKE_CI_USER_PASSWORD,
        database=SNOWFLAKE_CI_DATABASE,
        schema=SCHEMA_NAME,
    )
    cursor = connection.cursor()

    start = time.time()

    while True:
        query = f"select * from {LANDING_SCHEMA_NAME}.price"
        print(query)
        vendors = cursor.execute(query).fetchall()
        print(f"vendors in the landing table: {vendors}")

        if len(vendors) > 0:
            connection.execute_string(f"execute task {LANDING_SCHEMA_NAME}.INGEST_DATA")

        query = f"select * from {SCHEMA_NAME}.price"
        print(query)
        vendors = cursor.execute(query).fetchall()
        if len(vendors) > 0:
            print(f"found {len(vendors)} vendors")
            break
        if time.time() - start > 200:
            raise Exception(f"Timeout waiting for data ingest into Snowflake")
        print("Waiting for Snowlfake ingest")
        time.sleep(5)

    print("Deleting test schemas")
    cursor.execute(f"DROP SCHEMA {LANDING_SCHEMA_NAME}")
    cursor.execute(f"DROP SCHEMA {SCHEMA_NAME}")


if __name__ == "__main__":
    run_demo(
        "Snowfake Demo",
        os.path.join(SCRIPT_DIR, "project.sql"),
        make_config,
        prepare,
        verify,
    )
