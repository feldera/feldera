from tests import TEST_CLIENT
import time
import os
from confluent_kafka.admin import AdminClient
import requests
import re
import json

from tests.shared_test_pipeline import SharedTestPipeline, sql


def env(name: str, default: str) -> str:
    """Get environment variables for the Kafka broker and Schema registry.
    The default values are only meant for internal development; external users must set them."""
    return os.getenv(name, default)


# Set these before running the test:
# Example(terminal/shell):
#   export KAFKA_BOOTSTRAP_SERVERS= localhost:9092
#   export SCHEMA_REGISTRY_URL= http://localhost:8081

KAFKA_BOOTSTRAP = env(
    "KAFKA_BOOTSTRAP_SERVERS", "ci-kafka-bootstrap.korat-vibes.ts.net:9094"
)
SCHEMA_REGISTRY = env(
    "SCHEMA_REGISTRY_URL", "http://ci-schema-registry.korat-vibes.ts.net"
)


def extract_kafka_avro_artifacts(sql: str) -> tuple[list[str], list[str]]:
    """Extract Kafka topic and schema subjects from the SQL query"""
    topics = re.findall(r'"topic"\s*:\s*"([^"]+)"', sql)

    subjects = re.findall(r"create view\s+(\w+)", sql, re.I) + re.findall(
        r"create index\s+(\w+)", sql, re.I
    )

    return list(set(topics)), list(set(subjects))


def delete_kafka_topics(bootstrap_servers: str, topics: list[str]):
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    tpcs = admin.delete_topics(topics)

    for topic, tpcs in tpcs.items():
        try:
            tpcs.result()
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


def cleanup_kafka(sql: str, bootstrap_servers: str, registry_url: str):
    """Clean up Kafka topics and Schema Subjects after each test run.
    Each run produces new records. So, rerunning without cleanup will append data to the same topic(s)."""
    topics, subjects = extract_kafka_avro_artifacts(sql)
    delete_kafka_topics(bootstrap_servers, topics)
    delete_schema_subjects(registry_url, subjects)


class Variant:
    """Represents a pipeline variant whose tables and views share the same SQL but differ in connector configuration.
    Each variant generates unique topic, table, and view names based on the provided configuration."""

    def __init__(self, cfg):
        self.id = cfg["id"]
        self.limit = cfg["limit"]
        self.partitions = cfg.get("partitions")
        self.sync = cfg.get("sync")
        self.start_from = cfg.get("start_from")

        self.topic1 = f"my_topic_avro_{self.id}"
        self.topic2 = f"my_topic_avro2_{self.id}"
        self.source = f"t_{self.id}"
        self.view = f"v_{self.id}"
        self.loopback = f"loopback_{self.id}"


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
        "bootstrap.servers": "{KAFKA_BOOTSTRAP}",
        "topic": "{v.topic1}"
      }}
    }},
    "format": {{
      "name": "avro",
      "config": {{
        "update_format": "raw",
        "registry_urls": ["{SCHEMA_REGISTRY}"]
      }}
    }}
  }},
  {{
    "index": "idx_{v.id}",
    "transport": {{
      "name": "kafka_output",
      "config": {{
        "bootstrap.servers": "{KAFKA_BOOTSTRAP}",
        "topic": "{v.topic2}"
      }}
    }},
    "format": {{
      "name": "avro",
      "config": {{
        "update_format": "raw",
        "registry_urls": ["{SCHEMA_REGISTRY}"]
      }}
    }}
  }}]'
)
as select * from {v.source};

create index idx_{v.id} on {v.view}(id);
"""


def sql_loopback_table(v: Variant) -> str:
    # Optional configurations that will use connector defaults if not specified
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
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
        "registry_urls": ["{SCHEMA_REGISTRY}"]
      }}
    }}
  }}]'
);
"""


def build_sql(configs) -> str:
    """Generate SQL for the pipeline by combining all tables and view for each variant"""
    variants = [Variant(c) for c in configs]
    parts = []

    for v in variants:
        parts.append(sql_source_table(v))
        parts.append(sql_view(v))
        parts.append(sql_loopback_table(v))

    return "\n".join(parts)


def wait_for_rows(pipeline, expected_rows, timeout_s=1800, poll_interval_s=5):
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


def validate_loopback(self, variant: Variant):
    """Validation: once finished, the loopback table should contain all generated values
    Validate by comparing the hash of the source table 't' and loopback table"""
    src_tbl_hash = self.pipeline.query_hash(
        f"SELECT * FROM {variant.source} ORDER BY id, str"
    )

    loopback_tbl_hash = self.pipeline.query_hash(
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


class TestKafkaAvro(SharedTestPipeline):
    """Each test method uses its own SQL snippet and processes only its own variant."""

    TEST_CONFIGS = [
        {"id": 0, "limit": 10},
        {"id": 1, "limit": 20},
        # {
        #     "id": 2,
        #     "limit": 1000000,
        #     "partitions": [0],
        #     "sync": True,
        #     "start_from": "earliest",
        # },
    ]

    @sql(build_sql([TEST_CONFIGS[0]]))
    def test_kafka_avro_config_0(self):
        cfg = self.TEST_CONFIGS[0]
        variant = Variant(cfg)

        self.pipeline.start()
        try:
            expected_rows = variant.limit * 2  # view->Kafka + Kafka->loopback
            wait_for_rows(self.pipeline, expected_rows)
            validate_loopback(self, variant)
        finally:
            self.pipeline.stop(force=True)
            cleanup_kafka(build_sql([cfg]), KAFKA_BOOTSTRAP, SCHEMA_REGISTRY)

    @sql(build_sql([TEST_CONFIGS[1]]))
    def test_kafka_avro_config_1(self):
        cfg = self.TEST_CONFIGS[1]
        variant = Variant(cfg)

        self.pipeline.start()
        try:
            expected_rows = variant.limit * 2
            wait_for_rows(self.pipeline, expected_rows)
            validate_loopback(self, variant)
        finally:
            self.pipeline.stop(force=True)
            cleanup_kafka(build_sql([cfg]), KAFKA_BOOTSTRAP, SCHEMA_REGISTRY)
