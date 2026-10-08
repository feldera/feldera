import json

from confluent_kafka import Producer
from feldera.enums import FaultToleranceModel, PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from tests import KAFKA_BOOTSTRAP, TEST_CLIENT
from tests.kafka import kafka_topics
from .helper import gen_pipeline_name, wait_for_condition
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS


@gen_pipeline_name
def test_input_connectors(pipeline_name):
    """
    An input connector resides on a single host, but multiple input
    connectors for a single table are spread across hosts.  If the
    pipeline doesn't properly reshard data from multiple input connectors
    for a single table, this can cause unsoundness.

    This test checks for soundness with two input connectors that
    generate the same data for a single table.  If each one produces
    N records then a view should only be able to count N records, not
    2N.

    (This test passes with single-host also, of course.)
    """
    sql = """
CREATE TABLE Input1 (
    key VARCHAR NOT NULL PRIMARY KEY,
    number BIGINT NOT NULL
) with (
  'connectors' = '[{
    "name": "input1",
    "transport": {
      "name": "datagen",
      "config": {
          "plan": [{
              "limit": 2500
          }]
      }
    }
  },{
    "name": "input2",
    "transport": {
      "name": "datagen",
      "config": {
          "plan": [{
              "limit": 2500
          }]
      }
    }
  }]'
);

CREATE MATERIALIZED VIEW Count AS SELECT COUNT(*) from input1;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start()
    assert pipeline.status() == PipelineStatus.RUNNING

    pipeline.wait_for_completion(timeout_s=300)
    result = list(pipeline.query("SELECT * FROM Count;"))
    assert result == [{"EXPR$0": 2500}]


@gen_pipeline_name
def test_preprocessor_on_every_host(pipeline_name):
    """
    Test that preprocessors work with a multi-host setup.

    If the pipeline reaches the RUNNING state the preprocessor
    has been registered.
    """
    connector = """{
    "name": "%s",
    "transport": {
      "name": "datagen",
      "config": {
          "plan": [{
              "limit": 1
          }]
      }
    },
    "preprocessor": [{
      "name": "example",
      "message_oriented": true,
      "config": {}
    }]
  }"""
    sql = f"""
CREATE TABLE Input (
    key VARCHAR NOT NULL PRIMARY KEY,
    number BIGINT NOT NULL
) with (
  'connectors' = '[{connector % "input1"},{connector % "input2"}]'
);
"""
    udf_rust = """
use feldera_adapterlib::format::{ParseError, Splitter};
use feldera_adapterlib::preprocess::{
    Preprocessor, PreprocessorCreateError, PreprocessorFactory,
};
use feldera_types::preprocess::PreprocessorConfig;

pub struct ExamplePreprocessor;

impl Preprocessor for ExamplePreprocessor {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
        (data.to_vec(), vec![])
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(ExamplePreprocessor)
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        None
    }
}

pub struct ExamplePreprocessorFactory;

impl PreprocessorFactory for ExamplePreprocessorFactory {
    fn create(
        &self,
        _config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        Ok(Box::new(ExamplePreprocessor))
    }
}
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        udf_rust=udf_rust,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start()
    assert pipeline.status() == PipelineStatus.RUNNING
    pipeline.stop(force=True)


@gen_pipeline_name
def test_postprocessor_on_every_host(pipeline_name):
    """
    Test that postprocessors work with a multi-host setup.

    The coordinator allocates connectors in round-robin over hosts.
    We use two views over and hosts, checking that postprocessors
    are registered correctly.
    """
    connector = """{
    "name": "%s",
    "transport": {
      "name": "file_output",
      "config": {
          "path": "/dev/null"
      }
    },
    "format": { "name": "json" },
    "postprocessor": [{
      "name": "example",
      "config": {}
    }]
  }"""
    sql = f"""
CREATE TABLE Input (
    key VARCHAR NOT NULL PRIMARY KEY,
    number BIGINT NOT NULL
);
CREATE VIEW v1 WITH (
  'connectors' = '[{connector % "out1"}]'
) AS SELECT * FROM Input;
CREATE VIEW v2 WITH (
  'connectors' = '[{connector % "out2"}]'
) AS SELECT * FROM Input;
"""
    udf_rust = """
use feldera_adapterlib::postprocess::{
    Postprocessor, PostprocessorCreateError, PostprocessorFactory,
};
use feldera_types::postprocess::PostprocessorConfig;

pub struct ExamplePostprocessor;

impl Postprocessor for ExamplePostprocessor {
    fn push_buffer(&mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn fork(&self) -> Box<dyn Postprocessor> {
        Box::new(ExamplePostprocessor)
    }
}

pub struct ExamplePostprocessorFactory;

impl PostprocessorFactory for ExamplePostprocessorFactory {
    fn create(
        &self,
        _config: &PostprocessorConfig,
    ) -> Result<Box<dyn Postprocessor>, PostprocessorCreateError> {
        Ok(Box::new(ExamplePostprocessor))
    }
}
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        udf_rust=udf_rust,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start()
    assert pipeline.status() == PipelineStatus.RUNNING
    pipeline.stop(force=True)


@gen_pipeline_name
def test_output_gathers_every_host(pipeline_name):
    """
    A multihost pipeline gathers each view to the single host that owns
    its output connectors.  Only that host has the connectors, so the
    other hosts learn from it whether to send their part of the view.  If
    they did not, the connector would lose the records that they compute.

    This test inserts enough records that every host computes some of
    them, then checks that every record arrives at:

    - Output connectors that the views declare.  There are two views, so
      that the coordinator can assign their connectors to different hosts.

    - HTTP listeners that attach to views without connectors while the
      pipeline runs.

    The views are not materialized, because a materialized view always
    gathers from every host.
    """
    n_records = 1000

    # `/dev/null` does not work here: the file connector fails on it with
    # EINVAL, so it transmits nothing.
    connector = """{
    "name": "%s",
    "transport": {
      "name": "file_output",
      "config": {
          "path": "/tmp/%s-%s.json"
      }
    },
    "format": { "name": "json" }
  }"""
    sql = f"""
CREATE TABLE t (x BIGINT NOT NULL PRIMARY KEY);
CREATE VIEW v1 WITH (
  'connectors' = '[{connector % ("out1", pipeline_name, "out1")}]'
) AS SELECT * FROM t;
CREATE VIEW v2 WITH (
  'connectors' = '[{connector % ("out2", pipeline_name, "out2")}]'
) AS SELECT * FROM t;
CREATE VIEW l1 AS SELECT * FROM t;
CREATE VIEW l2 AS SELECT * FROM t;
"""

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()

    pipeline.start()
    listeners = {view: pipeline.listen(view) for view in ["l1", "l2"]}
    pipeline.input_json("t", [{"x": x} for x in range(n_records)])

    def transmitted_records(view: str, connector: str) -> int:
        stats = pipeline.output_connector_stats(view, connector)
        return stats.metrics.transmitted_records or 0

    for view, connector in [("v1", "out1"), ("v2", "out2")]:
        wait_for_condition(
            f"{view}.{connector} transmits {n_records} records",
            lambda: transmitted_records(view, connector) >= n_records,
            timeout_s=120.0,
            poll_interval_s=1.0,
        )
        assert transmitted_records(view, connector) == n_records

    for view, listener in listeners.items():
        received = []

        def received_all() -> bool:
            received.extend(record["x"] for record in listener.to_dict())
            return len(received) >= n_records

        wait_for_condition(
            f"listener on {view} receives {n_records} records",
            received_all,
            timeout_s=120.0,
            poll_interval_s=1.0,
        )
        assert sorted(received) == list(range(n_records))

    pipeline.stop(force=True)


@gen_pipeline_name
def test_distributed_kafka_input(pipeline_name):
    """
    A distributed Kafka input connector runs on every host, and each host
    reads different partitions of the topic.  Together, the hosts must read
    each record exactly once.

    The table has no primary key, so a record that two hosts read would count
    twice.  The topic has more partitions than hosts, so that every host has
    some partitions, and a number of partitions that is not a multiple of the
    number of hosts, so that the hosts read different numbers of partitions.

    (This test passes with single-host also, where the one host reads all of
    the partitions.)
    """
    n_records = 3000
    n_partitions = 2 * FELDERA_TEST_NUM_HOSTS + 1

    with kafka_topics("distributed-input", num_partitions=n_partitions) as [topic]:
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
        for record_id in range(n_records):
            producer.produce(
                topic,
                value=json.dumps({"id": record_id}).encode("utf-8"),
                partition=record_id % n_partitions,
            )
        assert producer.flush(timeout=30) == 0

        connector = {
            "name": "kafka_in",
            "distributed": True,
            "transport": {
                "name": "kafka_input",
                "config": {
                    "topic": topic,
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "start_from": "earliest",
                },
            },
            "format": {
                "name": "json",
                "config": {"update_format": "raw", "array": False},
            },
        }
        sql = f"""
CREATE TABLE t (id BIGINT NOT NULL) WITH (
  'connectors' = '{json.dumps([connector])}'
);
CREATE MATERIALIZED VIEW counts AS
  SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS n_distinct FROM t;
"""
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=None,
            ),
        ).create_or_replace()

        pipeline.start()
        try:

            def counts():
                return list(pipeline.query("SELECT n, n_distinct FROM counts"))

            # Kafka input never ends, so wait for the records to arrive.
            wait_for_condition(
                f"{n_records} records ingested",
                lambda: counts() == [{"n": n_records, "n_distinct": n_records}],
                timeout_s=120.0,
                poll_interval_s=1.0,
            )

            # The statistics list the connector once, with the records of all
            # of its hosts.
            inputs = [
                status
                for status in pipeline.stats().inputs
                if status.endpoint_name.endswith("kafka_in")
            ]
            assert len(inputs) == 1
            assert inputs[0].metrics.total_records == n_records

            # No host read a record twice, even after time to do so.
            assert counts() == [{"n": n_records, "n_distinct": n_records}]
        finally:
            pipeline.stop(force=True)


@gen_pipeline_name
def test_distributed_kafka_input_resume(pipeline_name):
    """
    A pipeline with a distributed Kafka input connector resumes from a
    checkpoint with each host reading the same partitions from where it left
    off: records produced while the pipeline was suspended arrive once, and
    records from before the checkpoint do not arrive again.

    (This test passes with single-host also.)
    """
    n_before = 1000
    n_after = 500
    n_partitions = 2 * FELDERA_TEST_NUM_HOSTS + 1

    with kafka_topics("distributed-input-resume", num_partitions=n_partitions) as [
        topic
    ]:
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

        def produce(record_ids):
            for record_id in record_ids:
                producer.produce(
                    topic,
                    value=json.dumps({"id": record_id}).encode("utf-8"),
                    partition=record_id % n_partitions,
                )
            assert producer.flush(timeout=30) == 0

        produce(range(n_before))
        connector = {
            "name": "kafka_in",
            "distributed": True,
            "transport": {
                "name": "kafka_input",
                "config": {
                    "topic": topic,
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    "start_from": "earliest",
                },
            },
            "format": {
                "name": "json",
                "config": {"update_format": "raw", "array": False},
            },
        }
        sql = f"""
CREATE TABLE t (id BIGINT NOT NULL) WITH (
  'materialized' = 'true',
  'connectors' = '{json.dumps([connector])}'
);
CREATE MATERIALIZED VIEW counts AS
  SELECT COUNT(*) AS n, COUNT(DISTINCT id) AS n_distinct FROM t;
"""
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
            ),
        ).create_or_replace()

        def counts():
            return list(pipeline.query("SELECT n, n_distinct FROM counts"))

        def wait_for_records(n):
            wait_for_condition(
                f"{n} records ingested",
                lambda: counts() == [{"n": n, "n_distinct": n}],
                timeout_s=120.0,
                poll_interval_s=1.0,
            )

        pipeline.start()
        try:
            wait_for_records(n_before)
            pipeline.checkpoint(wait=True)
            pipeline.stop(force=False)

            produce(range(n_before, n_before + n_after))
            pipeline.start()
            wait_for_records(n_before + n_after)

            # Nothing arrives twice, even after time to do so.
            assert counts() == [
                {"n": n_before + n_after, "n_distinct": n_before + n_after}
            ]
        finally:
            pipeline.stop(force=True)
