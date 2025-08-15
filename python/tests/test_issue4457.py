import unittest

from pandas import Timestamp
from feldera import PipelineBuilder
from tests import TEST_CLIENT


class TestIssue_4457(unittest.TestCase):
    def test_local(self):
        sql = """
        CREATE TABLE test_events (
            id VARCHAR NOT NULL PRIMARY KEY,
            a VARCHAR,
            t TIMESTAMP NOT NULL LATENESS INTERVAL 1 MINUTE
        );
        CREATE VIEW V AS SELECT * FROM test_events;
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT, name="test_issue4457", sql=sql
        ).create_or_replace()

        # TODO: use .query() instead
        out = pipeline.listen("v")

        pipeline.start()

        pipeline.input_json(
            "test_events",
            [{"id": "a", "a": "test4", "t": "2025-05-20 21:00:17.920"}],
        )
        pipeline.wait_for_idle()

        output = out.to_dict()
        assert output == [
            {
                "id": "a",
                "a": "test4",
                "t": Timestamp("2025-05-20 21:00:17.920"),
                "insert_delete": 1,
            }
        ]

        pipeline.input_json(
            "test_events",
            [{"id": "a", "a": "test5", "t": "2025-03-20 21:00:17.920"}],
        )
        pipeline.wait_for_idle()

        output = out.to_dict()
        assert output == []

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
