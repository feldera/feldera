import unittest

from feldera.pipeline_builder import PipelineBuilder
from feldera.testutils import unique_pipeline_name
from tests import TEST_CLIENT
from feldera.enums import PipelineStatus


class TestIssue4895(unittest.TestCase):
    def test_issue4895(self):
        """
        LATENESS is applied correctly to materialized tables
        """

        pipeline_name = unique_pipeline_name("test_issue4895")

        sql = """
        create table t (x int lateness 0) with
        ('materialized' = 'true');
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, pipeline_name, sql=sql
        ).create_or_replace()

        pipeline.start()
        assert pipeline.status() == PipelineStatus.RUNNING
        pipeline.input_json("t", {"x": 1})
        pipeline.input_json("t", {"x": 2})
        pipeline.input_json("t", {"x": 3})

        result = list(pipeline.query("SELECT * FROM t;"))
        assert len(result) == 3

        # Late row should have no effect
        pipeline.input_json("t", {"x": 0})
        result = list(pipeline.query("SELECT * FROM t;"))
        assert len(result) == 3

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
