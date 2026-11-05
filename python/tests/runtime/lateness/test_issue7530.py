import unittest

from feldera.pipeline_builder import PipelineBuilder
from feldera.testutils import (
    FELDERA_TEST_NUM_WORKERS,
    FELDERA_TEST_NUM_HOSTS,
)
from tests import TEST_CLIENT
from tests.platform.helper import PipelineTestCase
from feldera.runtime_config import RuntimeConfig


class TestIssue7530(PipelineTestCase):
    def test_issue7530(self):
        """
        A partial update of a late key is rejected and recorded in ERROR_VIEW with the key
        """

        pipeline_name = self.register_for_cleanup("test_issue7530")

        sql = """
        CREATE TABLE t (
          id INT NOT NULL,
          ts TIMESTAMP NOT NULL LATENESS INTERVAL 1 HOUR,
          v  VARCHAR,
          PRIMARY KEY (id, ts)
        );
        CREATE MATERIALIZED VIEW contents AS SELECT id, v FROM t;
        CREATE MATERIALIZED VIEW errors AS SELECT * FROM ERROR_VIEW;
        """

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
        # input_json waits until the pipeline processes its input, so the
        # pipeline receives each row after the previous one.
        pipeline.input_json("t", {"id": 1, "ts": "2024-01-01 00:00:00", "v": "a"})
        pipeline.input_json("t", {"id": 2, "ts": "2024-01-01 03:00:00", "v": "b"})

        # The waterline is now 02:00, so key (1, 00:00) is late.
        pipeline.input_json(
            "t",
            {"update": {"id": 1, "ts": "2024-01-01 00:00:00", "v": "c"}},
            update_format="insert_delete",
        )

        contents = sorted(
            pipeline.query("SELECT * FROM contents;"), key=lambda row: row["id"]
        )
        assert contents == [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]

        errors = list(pipeline.query("SELECT * FROM errors;"))
        assert errors == [
            {
                "table_or_view_name": "t",
                "message": "Late row",
                "metadata": "(1, 2024-01-01 00:00:00)",
            }
        ]

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
