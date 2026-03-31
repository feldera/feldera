import unittest

from feldera import PipelineBuilder
from feldera.testutils import unique_pipeline_name
from tests import TEST_CLIENT
import time
import os
from confluent_kafka.admin import AdminClient, NewTopic
import requests
import re
import json
import socket
import uuid
from datetime import datetime, timedelta

_KAFKA_BOOTSTRAP = None
_SCHEMA_REGISTRY = None
_KAFKA_ADMIN = None


# Set these before running the test:
# Example(terminal/shell):
#   export KAFKA_BOOTSTRAP_SERVERS= localhost:9092
#   export SCHEMA_REGISTRY_URL= http://localhost:8081


def env(name: str, default: str, check_http: bool = False) -> str:
    """Get environment variables used to configure the Kafka broker or Schema Registry endpoint.
    If the environment variables are not set, the default values are used; intended for internal development only.
    External users are expected to explicitly configure these variables."""
    value = os.getenv(name, default)

    if value == default:
        try:
            if check_http:
                # Check if Schema Registry is available
                requests.get(value, timeout=2).raise_for_status()
            else:
                # Check if Kafka broker is available
                if "://" in value:
                    # Remove protocol prefix if present (e.g., "kafka://host:port")
                    value = value.split("://", 1)[1]
                host, port = value.split(":")
                with socket.create_connection((host, int(port)), timeout=2):
                    pass  # just testing connectivity
        except Exception as e:
            raise RuntimeError(
                f"{name} is set to default '{default}', but cannot connect to it! ({e})"
            )

    return value


"""Kafka bootstrap, schema registry, and Kafka admin client are lazy-initialized to avoid triggering live
HTTP/socket connections when importing this file. Connections are only created when the test calls the
respective getter functions at runtime."""


def get_kafka_bootstrap() -> str:
    global _KAFKA_BOOTSTRAP
    if _KAFKA_BOOTSTRAP is None:
        _KAFKA_BOOTSTRAP = env(
            "KAFKA_BOOTSTRAP_SERVERS", "ci-kafka-bootstrap.korat-vibes.ts.net:9094"
        )
    return _KAFKA_BOOTSTRAP


def get_schema_registry() -> str:
    global _SCHEMA_REGISTRY
    if _SCHEMA_REGISTRY is None:
        _SCHEMA_REGISTRY = env(
            "SCHEMA_REGISTRY_URL",
            "http://ci-schema-registry.korat-vibes.ts.net",
            check_http=True,
        )
    return _SCHEMA_REGISTRY


def get_kafka_admin() -> AdminClient:
    global _KAFKA_ADMIN
    if _KAFKA_ADMIN is None:
        _KAFKA_ADMIN = AdminClient({"bootstrap.servers": get_kafka_bootstrap()})
    return _KAFKA_ADMIN


def extract_kafka_schema_artifacts(sql: str) -> tuple[list[str], list[str]]:
    """Extract Kafka topic and schema subjects from the SQL query"""
    topics = re.findall(r'"topic"\s*:\s*"([^"]+)"', sql)

    subjects = re.findall(r"create view\s+(\w+)", sql, re.I) + re.findall(
        r"create index\s+(\w+)", sql, re.I
    )

    return list(set(topics)), list(set(subjects))


def delete_kafka_topics(admin: AdminClient, topics: list[str]):
    tpcs = admin.delete_topics(topics)

    for topic, fut in tpcs.items():
        try:
            fut.result()
            print(f"Deleted topic: {topic}")
        except Exception as e:
            print(f"Failed to delete {topic}: {e}")


def delete_schema_subjects(registry_url: str, subjects: list[str]):
    for subject in subjects:
        r = requests.delete(f"{registry_url}/subjects/{subject}")
        print(
            f"Deleted schema subject: {subject}"
            if r.status_code == 200
            else f"Failed to delete {subject}: {r.status_code} {r.text}"
        )


def cleanup_kafka_schema_artifacts(sql: str, admin: AdminClient, registry_url: str):
    """Clean up Kafka topics and Schema Subjects after each test run.
    Each run produces new records. So, rerunning without cleanup will append data to the same topic(s)."""
    topics, subjects = extract_kafka_schema_artifacts(sql)
    delete_kafka_topics(admin, topics)
    delete_schema_subjects(registry_url, subjects)


def create_kafka_topic(topic_name: str, num_partitions: int, replication_factor: int):
    """Create new topics when multiple partitions are required, since the Kafka output connector does not support
    specifying the number of partitions during topic creation."""
    new_topic = NewTopic(
        topic_name, num_partitions=num_partitions, replication_factor=replication_factor
    )
    tpcs = get_kafka_admin().create_topics([new_topic])
    for topic, fut in tpcs.items():
        try:
            fut.result()
            print(
                f"Topic {topic} created with {num_partitions} partitions and {replication_factor} replication factor"
            )
        except Exception as e:
            if "already exists" in str(e):
                print(f"Topic {topic} already exists")
            else:
                raise


class Variant:
    """Represents a pipeline variant whose tables and views share the same SQL but differ in connector configuration.
    Each variant generates unique topic, table, and view names based on the provided configuration."""

    def __init__(self, cfg, pipeline_name):
        self.id = cfg["id"]
        self.limit = cfg["limit"]
        self.partitions = cfg.get("partitions")
        self.sync = cfg.get("sync")
        self.start_from = cfg.get("start_from")
        self.create_topic = cfg.get("create_topic", False)
        self.num_partitions = cfg.get("num_partitions")
        self.replication_factor = cfg.get("replication_factor")

        # Include date for age tracking
        date_str = datetime.now().strftime("%Y%m%d")
        suffix = uuid.uuid4().hex[:4]

        self.pipeline_name = pipeline_name

        self.topic1 = f"{self.pipeline_name}_topic_{date_str}_{suffix}"
        self.topic2 = f"{self.pipeline_name}_topic2_{date_str}_{suffix}"
        self.source = f"{self.pipeline_name}_table"
        self.view = f"{self.pipeline_name}_view"
        self.index = f"{self.pipeline_name}_idx_{suffix}"
        self.loopback = f"{self.pipeline_name}_loopback"


def sql_source_table(v: Variant) -> str:
    return f"""
create table {v.source} (
    id int,
    str varchar,
    dec decimal,
    reall real,
    dbl double,
    booll boolean,
    tmestmp timestamp,
    datee date,
    tme time
) with (
  'materialized' = 'true',
  'connectors' = '[{{
    "transport": {{
      "name": "datagen",
      "config": {{ "plan": [{{"limit": {v.limit}}}], "seed": 1 }}
    }}
  }}]'
);
"""


def sql_view(v: Variant) -> str:
    return f"""
create view {v.view}
with (
  'connectors' = '[{{
    "transport": {{
      "name": "kafka_output",
      "config": {{
        "bootstrap.servers": "{get_kafka_bootstrap()}",
        "topic": "{v.topic1}"
      }}
    }},
    "format": {{
      "name": "avro",
      "config": {{
        "update_format": "raw",
        "registry_urls": ["{get_schema_registry()}"]
      }}
    }}
  }},
  {{
    "index": "{v.index}",
    "transport": {{
      "name": "kafka_output",
      "config": {{
        "bootstrap.servers": "{get_kafka_bootstrap()}",
        "topic": "{v.topic2}"
      }}
    }},
    "format": {{
      "name": "avro",
      "config": {{
        "update_format": "raw",
        "registry_urls": ["{get_schema_registry()}"]
      }}
    }}
  }}]'
)
as select * from {v.source};

create index {v.index} on {v.view}(id);
"""


def sql_loopback_table(v: Variant) -> str:
    # Optional configurations that will use connector defaults if not specified
    config = {
        "bootstrap.servers": get_kafka_bootstrap(),
        "topic": v.topic2,
    }

    if v.start_from:
        config["start_from"] = v.start_from
    if v.partitions:
        config["partitions"] = v.partitions
    if v.sync:
        config["synchronize_partitions"] = v.sync

    # Convert to SQL config string
    config_json = json.dumps(config)

    return f"""
create table {v.loopback} (
    id int,
    str varchar,
    dec decimal,
    reall real,
    dbl double,
    booll boolean,
    tmestmp timestamp,
    datee date,
    tme time
) with (
  'materialized' = 'true',
  'connectors' = '[{{
    "transport": {{
      "name": "kafka_input",
       "config": {config_json}
    }},
    "format": {{
      "name": "avro",
      "config": {{
        "update_format": "raw",
        "registry_urls": ["{get_schema_registry()}"]
      }}
    }}
  }}]'
);
"""


def build_sql(v: Variant) -> str:
    """Generate SQL for the pipeline by combining all tables and view for each variant"""
    return "\n".join([sql_source_table(v), sql_view(v), sql_loopback_table(v)])


def wait_for_rows(pipeline, expected_rows, timeout_s=600, poll_interval_s=5):
    """Since records aren't processed instantaneously, wait until all rows are processed to validate completion by
    polling `total_completed_records` every `poll_interval_s` seconds until it reaches `expected_records`"""
    start = time.perf_counter()
    while True:
        stats = TEST_CLIENT.get_pipeline_stats(pipeline.name)
        completed = stats["global_metrics"]["total_completed_records"]
        print(f"Processed {completed}/{expected_rows} rows so far...")
        if completed >= expected_rows:
            return completed
        # Prevent infinite polling
        if time.perf_counter() - start > timeout_s:
            raise AssertionError(
                f"Timeout: only {completed}/{expected_rows} rows processed"
            )
        time.sleep(poll_interval_s)


def validate_loopback(pipeline, variant: Variant):
    """Validation: once finished, the loopback table should contain all generated values
    Validate by comparing the hash of the source table 't' and loopback table"""
    src_tbl_hash = pipeline.query_hash(
        f"SELECT * FROM {variant.source} ORDER BY id, str"
    )

    loopback_tbl_hash = pipeline.query_hash(
        f"SELECT * FROM {variant.loopback} ORDER BY id, str"
    )

    assert src_tbl_hash == loopback_tbl_hash, (
        f"Loopback table hash mismatch for variant {variant.id}!\n"
        f"Source table: {variant.source}\n"
        f"Loopback table: {variant.loopback}\n"
        f"Expected hash: {src_tbl_hash}\n"
        f"Got hash: {loopback_tbl_hash}"
    )

    print(f"Loopback table validated successfully for variant {variant.id}")


def poll_until_topic_exists(admin, topic_name, timeout=30):
    """Poll until the Kafka topic is created by the connector"""
    start = time.time()
    while time.time() - start < timeout:
        if topic_name in admin.list_topics().topics:
            return
        time.sleep(1)
    raise TimeoutError(f"Topic {topic_name} not created within {timeout}s")


def create_and_run_pipeline_variant(cfg):
    """Create and run multiple pipelines based on configurations defined for each pipeline variant"""
    pipeline_name = unique_pipeline_name(f"test_kafka_avro_{cfg['id']}")
    v = Variant(cfg, pipeline_name)

    # Pre-create topics if specified
    if v.create_topic:
        create_kafka_topic(v.topic1, v.num_partitions, v.replication_factor)
        create_kafka_topic(v.topic2, v.num_partitions, v.replication_factor)

    sql = build_sql(v)
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, sql).create_or_replace()

    try:
        pipeline.start()

        # For variants without pre-created topics, wait for the output connector to create them
        if not v.create_topic:
            admin = get_kafka_admin()
            poll_until_topic_exists(admin, v.topic1)
            poll_until_topic_exists(admin, v.topic2)

        # NOTE => total_completed_records counts all rows that are processed through each output as follows:
        # 1. Written by the view<v> -> Kafka
        # 2. Ingested into loopback table from Kafka
        # Thus, expected_records = generated_rows * number_of_outputs (in this case 2)
        expected_rows = v.limit * 2
        wait_for_rows(pipeline, expected_rows)
        validate_loopback(pipeline, v)
    finally:
        pipeline.stop(force=True)
        cleanup_kafka_schema_artifacts(sql, get_kafka_admin(), get_schema_registry())


class TestKafkaAvro(unittest.TestCase):
    """Each test method uses its own SQL snippet and processes only its own variant."""

    TEST_CONFIGS = [
        {
            "id": 0,
            "limit": 10,
            "sync": False,
            "partitions": [0],
            "start_from": "earliest",
        },
        {
            "id": 1,
            "limit": 1000000,
            "partitions": [0, 1, 2],
            "sync": False,
            "create_topic": True,
            "start_from": "earliest",
            "num_partitions": 3,  # pre-create topic with 3 partitions
            "replication_factor": 1,
        },
    ]

    @classmethod
    def setUpClass(cls):
        """Clean up stale topics older than 3 days from previous test runs"""

        admin = get_kafka_admin()
        cutoff = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")

        topics_to_delete = []
        for topic in admin.list_topics().topics:
            if "kafka_avro" in topic:
                try:
                    if topic.split("_")[4] < cutoff:
                        topics_to_delete.append(topic)
                except IndexError:
                    # Skip if topic doesn't have expected format
                    pass

        if topics_to_delete:
            print(f"Deleting {len(topics_to_delete)} stale topics: {topics_to_delete}")
            delete_kafka_topics(admin, topics_to_delete)

    def test_kafka_avro_variants(self):
        # If a run ID is specified, only the test with the specified run ID is ran
        run_id = os.getenv("RUN_ID")
        configs_to_run = (
            [cfg for cfg in self.TEST_CONFIGS if cfg["id"] == int(run_id)]
            if run_id is not None
            else self.TEST_CONFIGS
        )

        for cfg in configs_to_run:
            print(f"\n Running pipeline variant id = {cfg['id']}")
            create_and_run_pipeline_variant(cfg)


# To run all pipelines in this test file:
#   python -m pytest ./tests/workloads/test_kafka_avro.py

# To run a specific pipeline variant by its ID:
#   RUN_ID=0 python -m pytest ./tests/workloads/test_kafka_avro.py
