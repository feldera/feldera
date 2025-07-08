import os
import time
import unittest
import uuid

import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic

from feldera import PipelineBuilder
from tests import TEST_CLIENT, KAFKA_SERVER, PIPELINE_TO_KAFKA_SERVER


class TestPipelineBuilder(unittest.TestCase):
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

        name = "test_connector_orchestration"

        pipeline = PipelineBuilder(TEST_CLIENT, name, sql=sql).create_or_replace()
        pipeline.start()

        pipeline.resume_connector("numbers", "c1")
        stats = TEST_CLIENT.get_pipeline_stats(name)
        c1_status = next(
            item["paused"]
            for item in stats["inputs"]
            if item["endpoint_name"] == "numbers.c1"
        )
        assert not c1_status

        pipeline.pause_connector("numbers", "c1")
        stats = TEST_CLIENT.get_pipeline_stats(name)
        c2_status = next(
            item["paused"]
            for item in stats["inputs"]
            if item["endpoint_name"] == "numbers.c1"
        )
        assert c2_status

        pipeline.stop(force=True)
        pipeline.clear_storage()


if __name__ == "__main__":
    unittest.main()
