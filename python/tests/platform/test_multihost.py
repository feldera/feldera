from feldera.enums import PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from tests import TEST_CLIENT
from .helper import gen_pipeline_name
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
