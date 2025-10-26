import unittest

from feldera import PipelineBuilder
from tests import TEST_CLIENT, unique_pipeline_name


class TestTransactions(unittest.TestCase):
    def test_dynamic_output_connector(self):
        # When an output connector is created during a transaction, it should not receive any outputs until the next transaction.

        sql = """
        CREATE TABLE t1 (
          id INT NOT NULL
        );
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=unique_pipeline_name("test_dynamic_output_connector"),
            sql=sql,
        ).create_or_replace()

        pipeline.start()

        pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")

        pipeline.wait_for_completion()

        pipeline.start_transaction()
        pipeline.execute("INSERT INTO t1 VALUES (4), (5), (6);")
        # out should only start receiving outputs starting from the next transaction.
        out = pipeline.listen("t1")
        pipeline.execute("INSERT INTO t1 VALUES (7), (8), (9);")
        pipeline.commit_transaction()
        pipeline.wait_for_completion()

        pipeline.start_transaction()
        pipeline.execute("INSERT INTO t1 VALUES (10), (11), (12);")
        pipeline.commit_transaction()
        pipeline.wait_for_completion()

        output = out.to_dict()
        assert output == [
            {
                "id": 10,
                "insert_delete": 1,
            },
            {
                "id": 11,
                "insert_delete": 1,
            },
            {
                "id": 12,
                "insert_delete": 1,
            },
        ]

        pipeline.stop(force=True)

        pipeline.start()

        # out1 should observe all outputs.
        out1 = pipeline.listen("t1")
        pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")

        pipeline.wait_for_completion()

        pipeline.start_transaction()
        pipeline.execute("INSERT INTO t1 VALUES (4), (5), (6);")
        # out2 should only start receiving outputs starting from the next transaction.
        out2 = pipeline.listen("t1")
        pipeline.execute("INSERT INTO t1 VALUES (7), (8), (9);")
        pipeline.commit_transaction()
        pipeline.wait_for_completion()

        pipeline.start_transaction()
        pipeline.execute("INSERT INTO t1 VALUES (10), (11), (12);")
        # out3 should not receive any outputs.
        out3 = pipeline.listen("t1")
        pipeline.execute("INSERT INTO t1 VALUES (13), (14), (15);")
        pipeline.commit_transaction()
        pipeline.wait_for_completion()

        output = out1.to_dict()
        assert output == [
            {
                "id": 1,
                "insert_delete": 1,
            },
            {
                "id": 2,
                "insert_delete": 1,
            },
            {
                "id": 3,
                "insert_delete": 1,
            },
            {
                "id": 4,
                "insert_delete": 1,
            },
            {
                "id": 5,
                "insert_delete": 1,
            },
            {
                "id": 6,
                "insert_delete": 1,
            },
            {
                "id": 7,
                "insert_delete": 1,
            },
            {
                "id": 8,
                "insert_delete": 1,
            },
            {
                "id": 9,
                "insert_delete": 1,
            },
            {
                "id": 10,
                "insert_delete": 1,
            },
            {
                "id": 11,
                "insert_delete": 1,
            },
            {
                "id": 12,
                "insert_delete": 1,
            },
            {
                "id": 13,
                "insert_delete": 1,
            },
            {
                "id": 14,
                "insert_delete": 1,
            },
            {
                "id": 15,
                "insert_delete": 1,
            },
        ]

        output = out2.to_dict()
        assert output == [
            {
                "id": 10,
                "insert_delete": 1,
            },
            {
                "id": 11,
                "insert_delete": 1,
            },
            {
                "id": 12,
                "insert_delete": 1,
            },
            {
                "id": 13,
                "insert_delete": 1,
            },
            {
                "id": 14,
                "insert_delete": 1,
            },
            {
                "id": 15,
                "insert_delete": 1,
            },
        ]

        output = out3.to_dict()
        assert output == []

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
