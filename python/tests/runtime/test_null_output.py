"""Tests for the null_output connector."""

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from tests.platform.helper import gen_pipeline_name


@gen_pipeline_name
def test_null_output(pipeline_name):
    """Verify that a pipeline with a null_output connector runs to completion.

    The connector discards all output; correctness is verified by querying the
    materialized view directly via an ad-hoc query.
    """
    sql = """
CREATE TABLE t (id INT NOT NULL) WITH (
    'connectors' = '[{
        "name": "t",
        "transport": {
            "name": "datagen",
            "config": {"plan": [{"limit": 1000}]}
        }
    }]'
);

CREATE MATERIALIZED VIEW v
WITH (
    'connectors' = '[{
        "name": "null_out",
        "transport": {"name": "null_output"},
        "format": {"name": "json"}
    }]'
) AS SELECT COUNT(*) AS c FROM t;
""".strip()

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
        ),
    ).create_or_replace()

    pipeline.start()
    pipeline.wait_for_completion()

    rows = list(pipeline.query("SELECT c FROM v"))
    assert rows == [{"c": 1000}], f"Unexpected row count: {rows}"

    pipeline.stop(force=True)
    pipeline.delete(True)
