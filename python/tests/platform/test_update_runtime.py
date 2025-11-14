import unittest

from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import unique_pipeline_name
from tests import TEST_CLIENT
from feldera.enums import PipelineStatus


class TestPipeline(unittest.TestCase):
    def test_update_runtime(self):
        """
        Test the /update_runtime API.

        Pipeline's platform version should only be updated to the current version on-demand
        (by calling /update_runtime) or when updating the pipeline code.
        """

        pipeline_name = unique_pipeline_name("test_update_runtime")

        sql = """
        CREATE TABLE tbl(id INT) WITH ('materialized' = 'true');
CREATE MATERIALIZED VIEW v0 AS SELECT * FROM tbl;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                logging="debug",
            ),
        ).create_or_replace()

        # Make sure the pipeline gets compiled, so we can set its platform version
        # without having it overwriten by the compiler.
        pipeline.start()
        assert pipeline.status() == PipelineStatus.RUNNING
        pipeline.stop(force=True)

        pipeline.testing_force_update_platform_version("0.0.1")
        assert pipeline.platform_version() == "0.0.1"

        # Run pipeline compiled by a previous version of the platform without triggering recompilation.
        pipeline.start()

        assert pipeline.status() == PipelineStatus.RUNNING
        assert pipeline.platform_version() == "0.0.1"

        # update_runtime fails while the pipeline is running
        with self.assertRaises(Exception):
            pipeline.update_runtime()

        pipeline.stop(force=True)

        # update_runtime works when the pipeline is stopped
        pipeline.update_runtime()
        assert pipeline.platform_version() != "0.0.1"

        pipeline.start()
        assert pipeline.status() == PipelineStatus.RUNNING

        pipeline.stop(force=True)

        # Modifying program code updates platform version automatically.
        pipeline.testing_force_update_platform_version("0.0.1")
        assert pipeline.platform_version() == "0.0.1"

        pipeline.modify(sql=sql + "\nCREATE MATERIALIZED VIEW v1 AS SELECT * FROM tbl;")
        pipeline.start()
        assert pipeline.status() == PipelineStatus.RUNNING
        assert pipeline.platform_version() != "0.0.1"

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
