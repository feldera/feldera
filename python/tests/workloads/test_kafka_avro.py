import unittest
from tests import TEST_CLIENT
from feldera import PipelineBuilder
import time
import os
from confluent_kafka.admin import AdminClient
import requests
import re


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


# Set the limit for number of records to generate
LIMIT = 1000000


class TestKafkaAvro(unittest.TestCase):
    def test_check_avro(self):
        sql = f"""
create table t (
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
      "config": {{ "plan": [{{"limit": {LIMIT}}}], "seed": 1 }}
    }}
  }}]'
);

create view v
with (
  'connectors' = '[{{
    "transport": {{
      "name": "kafka_output",
      "config": {{
        "bootstrap.servers": "{KAFKA_BOOTSTRAP}",
        "topic": "my_topic_avro"
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
    "index": "t_index",
    "transport": {{
      "name": "kafka_output",
      "config": {{
        "bootstrap.servers": "{KAFKA_BOOTSTRAP}",
        "topic": "my_topic_avro2"
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
as select * from t;

create index t_index on v(id);

create table loopback (
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
      "config": {{
        "topic": "my_topic_avro2",
        "start_from": "earliest",
        "bootstrap.servers": "{KAFKA_BOOTSTRAP}"
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
);
"""
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            "test_kafka_avro",
            sql=sql,
        ).create_or_replace()

        try:
            pipeline.start()

            # NOTE => total_completed_records counts all rows that are processed through each output as follows:
            # 1. Written by the view<v> -> Kafka
            # 2. Ingested into loopback table from Kafka
            # Thus, expected_records = generated_rows * number_of_outputs (in this case 2)
            expected_records = LIMIT * 2
            timeout_s = 1800
            poll_interval_s = 5

            start_time = time.perf_counter()
            # Poll  `total_completed_records` every `poll_interval_s` seconds until it reaches `expected_records`
            while True:
                stats = TEST_CLIENT.get_pipeline_stats(pipeline.name)
                completed = stats["global_metrics"]["total_completed_records"]

                print(f"Processed {completed}/{expected_records} rows so far...")

                if completed >= expected_records:
                    break

                # Prevent infinite polling
                if time.perf_counter() - start_time > timeout_s:
                    raise AssertionError(
                        f"Timeout: only {completed}/{expected_records} rows processed"
                    )

                time.sleep(poll_interval_s)

            elapsed = time.perf_counter() - start_time
            print(
                f"All {completed}/{expected_records} rows processed in {elapsed:.3f}s"
            )

            # Validation: once finished, the loopback table should contain all generated values
            # Validate by comparing the hash of the source table 't' and loopback table

            expected_hash = pipeline.query_hash("SELECT * FROM t ORDER BY id, str")
            result_hash = pipeline.query_hash("SELECT * FROM loopback ORDER BY id, str")

            assert result_hash == expected_hash, (
                f"Validation failed: loopback table hash mismatch!\n"
                f"Expected: {expected_hash}\nGot: {result_hash}"
            )
            print("Loopback table validated successfully!")

        finally:
            pipeline.stop(force=True)

            # Cleanup Kafka and Schema Registry
            cleanup_kafka(sql, KAFKA_BOOTSTRAP, SCHEMA_REGISTRY)
