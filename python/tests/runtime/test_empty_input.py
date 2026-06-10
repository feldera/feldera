"""Tests for the empty_input connector."""

from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from tests.platform.helper import gen_pipeline_name


@gen_pipeline_name
def test_empty_input(pipeline_name):
    """Verify that a pipeline with an empty_input connector runs to completion.

    The connector immediately signals end of input; the view should contain
    no rows.
    """
    sql = """
CREATE TABLE t (id INT NOT NULL) WITH (
    'connectors' = '[{
        "name": "t",
        "transport": {"name": "empty_input"},
        "format": {"name": "json"}
    }]'
);

CREATE MATERIALIZED VIEW v AS SELECT COUNT(*) AS c FROM t;
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
    assert rows == [{"c": 0}], f"Unexpected row count: {rows}"

    pipeline.stop(force=True)
    pipeline.delete(True)
