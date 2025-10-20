import unittest
import time

from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import unique_pipeline_name
from tests import TEST_CLIENT
from feldera.enums import PipelineStatus

def get_result(pipeline) -> str:
    result = list(pipeline.query("SELECT * FROM v;"))
    assert len(result) == 1
    return result[0]['x']

class TestNowStream(unittest.TestCase):
    def test_nowstream(self):
        """
        Test the now() function:
        pipeline should produce outputs even if no new inputs are supplied.
        """

        pipeline_name = unique_pipeline_name("test_now")

        sql = """
        CREATE MATERIALIZED VIEW v AS SELECT NOW() as X;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                # 10 times per second
                clock_resolution_usecs=100000
            ),
        ).create_or_replace()

        pipeline.start()
        assert pipeline.status() == PipelineStatus.RUNNING
        time0 = get_result(pipeline)
        time.sleep(1)
        time1 = get_result(pipeline)
        # Time has increased; this works on string time representations too
        # due to the standard format, which looks like `2025-10-20T20:55:16.350`
        assert time1 > time0
        pipeline.stop(force=True)

if __name__ == "__main__":
    unittest.main()
