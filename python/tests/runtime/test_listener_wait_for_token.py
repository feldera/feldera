from feldera import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from tests.platform.helper import PipelineTestCase


def _sorted(changes):
    return sorted(changes, key=lambda change: sorted(change.items()))


class TestListenerWaitForToken(PipelineTestCase):
    def test_wait_for_token(self):
        """`OutputHandler.wait_for_token` returns once the listener holds every
        change for the token, including when the token changed nothing."""

        sql = """
        CREATE TABLE orders (customer VARCHAR NOT NULL, amount INT NOT NULL);
        CREATE VIEW large_orders AS SELECT * FROM orders WHERE amount >= 100;
        CREATE VIEW customer_totals AS
            SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer;
        """
        pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=self.register_for_cleanup("test_listener_wait_for_token"),
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
            ),
        ).create_or_replace()

        # Connect the listeners before the first step.
        pipeline.start_paused()
        totals = pipeline.listen("customer_totals")
        large = pipeline.listen("large_orders")
        pipeline.resume()

        token = pipeline.input_json("orders", [{"customer": "alice", "amount": 10}])
        totals.wait_for_token(token, timeout_s=60)
        large.wait_for_token(token, timeout_s=60)
        assert totals.to_dict() == [
            {"customer": "alice", "total": 10, "insert_delete": 1}
        ]
        # A small order changes `large_orders` not at all, and the wait
        # confirmed that every change for the token has arrived.
        assert large.to_dict() == []

        token = pipeline.input_json("orders", [{"customer": "alice", "amount": 100}])
        totals.wait_for_token(token, timeout_s=60)
        large.wait_for_token(token, timeout_s=60)
        assert _sorted(totals.to_dict()) == _sorted(
            [
                {"customer": "alice", "total": 10, "insert_delete": -1},
                {"customer": "alice", "total": 110, "insert_delete": 1},
            ]
        )
        assert large.to_dict() == [
            {"customer": "alice", "amount": 100, "insert_delete": 1}
        ]

        # A token that the handle already caught up with returns at once.
        totals.wait_for_token(token, timeout_s=60)
        assert totals.to_dict() == []
