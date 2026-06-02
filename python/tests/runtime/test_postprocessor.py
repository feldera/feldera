import json
import uuid
import unittest

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from feldera import PipelineBuilder
from tests import KAFKA_BOOTSTRAP, TEST_CLIENT
from tests.platform.helper import PipelineTestCase
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS
from typing import Any

LIMIT = 100
# Uncomment the following for local testing
# KAFKA_BOOTSTRAP = "ci-kafka-bootstrap.korat-vibes.ts.net:9094"


def _random_topic(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _create_topic(admin: Any, topic: str) -> None:
    futures = admin.create_topics(
        [NewTopic(topic=topic, num_partitions=1, replication_factor=1)]
    )
    futures[topic].result(timeout=30)


def _delete_topic(admin: Any, topic: str) -> None:
    try:
        futures = admin.delete_topics([topic], operation_timeout=10)
        futures[topic].result(timeout=10)
    except Exception:
        pass  # Topic deletion can be disabled on some brokers; cleanup is best-effort.


def _consume_all(topic: str, timeout_s: float = 30.0) -> list[dict]:
    """Read all messages from a topic (pipeline has already stopped)."""
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"test-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            # Necessary to detect end-of-partition in the loop below
            "enable.partition.eof": True,
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    messages = []
    while True:
        msg = consumer.poll(timeout=10.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            raise RuntimeError(f"Kafka consumer error: {msg.error()}")
        for line in msg.value().splitlines():
            messages.append(json.loads(line))
    consumer.close()
    return messages


# Test user-defined postprocessor that modifies output data.
# The postprocessor runs on a kafka_output connector; reading back from
# Kafka lets us verify the scaled values it wrote.
class TestPostprocessor(PipelineTestCase):
    def test_postprocessor(self):
        if True:
            # Disabled https://github.com/feldera/feldera/issues/6460
            return
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        output_topic = _random_topic("postprocessor-out")
        _create_topic(admin, output_topic)

        sql = f"""
CREATE TABLE t (i BIGINT) WITH ('connectors' = '[{{
   "name": "t",
   "transport": {{
      "name": "datagen",
      "config": {{
         "plan": [{{
            "limit": {LIMIT},
            "fields": {{ "i": {{"strategy": "increment", "range": [0, {LIMIT - 1}]}} }}
         }}]
      }}
   }}
}}]');

CREATE MATERIALIZED VIEW v
WITH (
    'connectors' = '[{{
       "name": "v",
       "transport": {{
          "name": "kafka_output",
          "config": {{
             "topic": "{output_topic}",
             "bootstrap.servers": "{KAFKA_BOOTSTRAP}",
             "auto.offset.reset": "earliest"
         }}
       }},
       "format": {{ "name": "json" }},
       "postprocessor": [{{
          "name": "scale",
          "config": {{}}
       }}]
    }}]'
) AS SELECT SUM(i) AS sum FROM t;
        """

        udfs = r"""
use feldera_adapterlib::postprocess::{
    Postprocessor, PostprocessorCreateError, PostprocessorFactory,
};
use feldera_types::postprocess::PostprocessorConfig;
use tracing::info;

pub struct ScalePostprocessor;

/// Postprocessor that multiplies the value of "sum" in each JSON record by 10.
/// Records arrive in insert_delete format: {"insert":{"sum":X}} or {"delete":{"sum":X}}.
impl Postprocessor for ScalePostprocessor {
    fn push_buffer(&mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut result = Vec::with_capacity(data.len());
        for line in data.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let mut record: serde_json::Value = serde_json::from_slice(line)?;
            info!("before {:?}", record);
            for key in &["insert", "delete"] {
                if let Some(inner) = record.get_mut(*key) {
                    if let Some(v) = inner.get_mut("sum") {
                        if let Some(n) = v.as_i64() {
                            *v = serde_json::Value::Number(serde_json::Number::from(n * 10));
                        }
                    }
                }
            }
            info!("after {:?}", record);
            result.extend_from_slice(&serde_json::to_vec(&record)?);
            result.push(b'\n');
        }
        Ok(result)
    }

    fn fork(&self) -> Box<dyn Postprocessor> {
        Box::new(ScalePostprocessor)
    }
}

pub struct ScalePostprocessorFactory;

impl PostprocessorFactory for ScalePostprocessorFactory {
    fn create(
        &self,
        _config: &PostprocessorConfig,
    ) -> Result<Box<dyn Postprocessor>, PostprocessorCreateError> {
        Ok(Box::new(ScalePostprocessor))
    }
}
"""

        toml = """
tracing = { version = "0.1.40" }
"""

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=self.register_for_cleanup("test_postprocessor"),
            sql=sql,
            udf_rust=udfs,
            udf_toml=toml,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
            ),
        ).create_or_replace()

        try:
            pipeline.start_paused()
            pipeline.resume()
            pipeline.wait_for_completion()

            # Query the view directly to get the unmodified aggregate.
            rows = list(pipeline.query("SELECT sum FROM v"))
            assert len(rows) == 1, f"Expected 1 row, got {rows}"
            original_sum = rows[0]["sum"]

            # The last insert message written by the postprocessor holds the
            # scaled final aggregate.
            messages = _consume_all(output_topic)
            assert messages, "No messages received from Kafka topic"
            last_insert = next(
                (m["insert"] for m in reversed(messages) if "insert" in m), None
            )
            assert last_insert is not None, f"No insert message found in {messages}"
            assert last_insert == {"sum": original_sum * 10}, (
                f"Expected {{'sum': {original_sum * 10}}}, got {last_insert}"
            )
        finally:
            pipeline.stop(force=True)
            # Pipeline will be reaped in 24 hours automatically
            _delete_topic(admin, output_topic)


if __name__ == "__main__":
    unittest.main()
