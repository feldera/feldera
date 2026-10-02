from feldera.enums import PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from tests import TEST_CLIENT
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
