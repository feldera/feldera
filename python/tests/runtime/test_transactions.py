import unittest
import uuid

from feldera import PipelineBuilder
from feldera.rest.errors import FelderaAPIError
from tests import TEST_CLIENT
from tests.platform.helper import PipelineTestCase, wait_for_records
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_WORKERS, FELDERA_TEST_NUM_HOSTS


class TestTransactions(PipelineTestCase):
    def test_dynamic_output_connector(self):
        # When an output connector is created during a transaction, it should not receive any outputs until the next transaction.

        sql = """
        CREATE TABLE t1 (
          id INT NOT NULL
        );
        """

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=self.register_for_cleanup("test_dynamic_output_connector"),
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
            ),
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

        wait_for_records(out, 3)
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

        wait_for_records(out1, 15)
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

        wait_for_records(out2, 6)
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

        # No wait here: out3 is asserted to stay empty, and the waits above give
        # it every chance to receive the records its siblings already saw.
        output = out3.to_dict()
        assert output == []

        pipeline.stop(force=True)

    @unittest.skipIf(
        FELDERA_TEST_NUM_HOSTS > 1,
        "the multihost coordinator does not check the transaction ID yet",
    )
    def test_commit_transaction_by_id(self):
        """A commit names its transaction and incarnation.  A retry of it
        succeeds, and a commit of another transaction or incarnation fails."""

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=self.register_for_cleanup("test_commit_transaction_by_id"),
            sql="CREATE TABLE t1 (id INT NOT NULL) WITH ('materialized' = 'true');",
            runtime_config=RuntimeConfig(workers=FELDERA_TEST_NUM_WORKERS),
        ).create_or_replace()
        pipeline.start()

        started = pipeline.start_transaction_response()
        tid = started["transaction_id"]
        incarnation = started["incarnation_uuid"]
        assert incarnation == str(pipeline.stats().global_metrics.incarnation_uuid)
        pipeline.execute("INSERT INTO t1 VALUES (1), (2), (3);")

        with self.assertRaises(FelderaAPIError) as error:
            pipeline.commit_transaction(tid, incarnation_uuid=str(uuid.uuid4()))
        assert error.exception.error_code == "IncarnationUuidMismatch"

        pipeline.commit_transaction(tid, incarnation_uuid=incarnation)
        pipeline.commit_transaction(tid, incarnation_uuid=incarnation)
        assert list(pipeline.query("SELECT COUNT(*) AS n FROM t1")) == [{"n": 3}]

        with self.assertRaises(FelderaAPIError) as error:
            pipeline.commit_transaction(tid + 1, incarnation_uuid=incarnation)
        assert error.exception.error_code == "TransactionIdMismatch"

        # Only the last commit can be retried.
        second = pipeline.start_transaction()
        pipeline.commit_transaction(second, incarnation_uuid=incarnation)
        with self.assertRaises(FelderaAPIError) as error:
            pipeline.commit_transaction(tid, incarnation_uuid=incarnation)
        assert error.exception.error_code == "TransactionIdMismatch"

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
