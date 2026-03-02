import unittest
from tests import TEST_CLIENT
from feldera import PipelineBuilder
from feldera.enums import CompilationProfile
import time


class TestKafkaAvro(unittest.TestCase):
    def test_check_avro(self):
        sql = """
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
  'connectors' = '[{
    "transport": {
      "name": "datagen",
      "config": { "plan": [{"limit": 1000000}] }
    }
  }]'
);

create view v
with (
  'connectors' = '[{
    "transport": {
      "name": "kafka_output",
      "config": {
        "bootstrap.servers": "ci-kafka-bootstrap.korat-vibes.ts.net:9094",
        "topic": "my_topic_avro"
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "update_format": "raw",
        "registry_urls": ["http://ci-schema-registry.korat-vibes.ts.net"]
      }
    }
  },
    {
    "index": "t_index",
    "transport": {
      "name": "kafka_output",
      "config": {
        "bootstrap.servers": "ci-kafka-bootstrap.korat-vibes.ts.net:9094",
        "topic": "my_topic_avro2"
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "update_format": "raw",
        "registry_urls": ["http://ci-schema-registry.korat-vibes.ts.net"]
      }
    }
}]'
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
  'connectors' = '[{
    "transport": {
      "name": "kafka_input",
      "config": {
        "topic": "my_topic_avro2",
        "start_from": "earliest",
        "bootstrap.servers": "ci-kafka-bootstrap.korat-vibes.ts.net:9094"
      }
    },
    "format": {
      "name": "avro",
      "config": {
        "update_format": "raw",
        "registry_urls": ["http://ci-schema-registry.korat-vibes.ts.net"]
      }
    }
  }]'
);
"""
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            "test_kafka_avro",
            sql=sql,
            compilation_profile=CompilationProfile.DEV,
        ).create_or_replace()

        try:
            pipeline.start()

            # NOTE => total_completed_records counts all rows that are processed through each output as follows:
            # 1. Written by the view<v> -> Kafka
            # 2. Ingested into loopback table from Kafka
            # Thus, expected_records = generated_rows * number_of_outputs (in this case 2)
            expected_records = 2000000
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
            count_table = list(pipeline.query("SELECT COUNT(*) AS count FROM t"))[0][
                "count"
            ]
            count_loopback = list(
                pipeline.query("SELECT COUNT(*) AS count FROM loopback")
            )[0]["count"]

            assert count_table == count_loopback, (
                f"Validation failed: {count_loopback} rows ingested vs {count_table} expected"
            )
            print(f"Loopback table validated successfully: {count_loopback} rows")

        finally:
            pipeline.stop(force=True)
