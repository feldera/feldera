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
