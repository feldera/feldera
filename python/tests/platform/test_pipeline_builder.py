import unittest
from feldera.testutils import (
    FELDERA_TEST_NUM_WORKERS,
    FELDERA_TEST_NUM_HOSTS,
)
from tests import TEST_CLIENT
from tests.platform.helper import PipelineTestCase
from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig


class TestPipelineBuilder(PipelineTestCase):
    def test_connector_orchestration(self):
        sql = """
        CREATE TABLE numbers (
          num INT
        ) WITH (
            'connectors' = '[
                {
                    "name": "c1",
                    "paused": true,
                    "transport": {
                        "name": "datagen",
                        "config": {"plan": [{ "rate": 1, "fields": { "num": { "range": [0, 10], "strategy": "uniform" } } }]}
                    }
                }
            ]'
        );
        """

        pipeline_name = self.register_for_cleanup("test_connector_orchestration")

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
            ),
        ).create_or_replace()
        pipeline.start()

        pipeline.resume_connector("numbers", "c1")
        stats = TEST_CLIENT.get_pipeline_stats(pipeline_name)
        c1_status = next(
            item["paused"]
            for item in stats["inputs"]
            if item["endpoint_name"] == "numbers.c1"
        )
        assert not c1_status

        pipeline.pause_connector("numbers", "c1")
        stats = TEST_CLIENT.get_pipeline_stats(pipeline_name)
        c2_status = next(
            item["paused"]
            for item in stats["inputs"]
            if item["endpoint_name"] == "numbers.c1"
        )
        assert c2_status

        pipeline.stop(force=True)
        pipeline.clear_storage()

    def test_tags(self):
        sql = "CREATE TABLE t (col1 INT);"

        # Omitting tags defaults to an empty list, never None.
        default_name = self.register_for_cleanup("test_builder_tags_default")
        default_pipeline = PipelineBuilder(
            TEST_CLIENT,
            default_name,
            sql=sql,
        ).create_or_replace()
        assert default_pipeline.tags() == []

        pipeline_name = self.register_for_cleanup("test_builder_tags")

        # Tags supplied to the builder are persisted and round-trip on read.
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            description="initial",
            tags=["prod", "team-billing"],
        ).create_or_replace()
        assert pipeline.tags() == ["prod", "team-billing"]
        assert pipeline.description() == "initial"

        # `modify` patches tags independently of the description.
        pipeline.modify(tags=["staging"])
        assert pipeline.tags() == ["staging"]
        assert pipeline.description() == "initial"

        # Patching the description leaves the tags untouched.
        pipeline.modify(description="changed")
        assert pipeline.description() == "changed"
        assert pipeline.tags() == ["staging"]

        # An empty list is a real value: it clears the tags.
        pipeline.modify(tags=[])
        assert pipeline.tags() == []

        # The SDK normalizes tags before storing them, matching the web console:
        # tags are saved in sorted order, and color variants of the same name
        # (the same text with different "|rrggbb" color suffixes) are collapsed to
        # the last one supplied. The surviving variant keeps its color.
        pipeline.modify(tags=["prod", "alpha", "prod|ef4444"])
        assert pipeline.tags() == ["alpha", "prod|ef4444"]


if __name__ == "__main__":
    unittest.main()
