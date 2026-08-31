"""Tests the model-scoring use case.

Runs an ML model as a separate process outside the pipeline. The two exchange data only
through one view (to the model) and one input table (from the model).

    1. predict on the seed data
    2. correct a cardholder record, re-predicting only the affected window
    3. a late fraud label arrives, moving the score without any new prediction

The SQL is read from the use-case directory.

    main thread                     pipeline                     model server
    (the test body)                                             (daemon thread)

                               datagen seeds all three input tables
                                           |
                                           v
                               transaction
    input_json  ------------>  cardholder
    input_json  ------------>  confirmed_fraud_label
                                           |
                                           v
                               fingerprinted_features
                                           |
                                           v
                               unpredicted_features  ------------>  scores the
                                           ^                        request
                        an answer removes  |                            |
                        its own request    |                            |
                               model_prediction  <----------------------+
                                           |
                                           v
                               predicted_transaction
    SELECT  <----------------  model_score
    polled until settled
"""

import math
import threading
import time
import unittest
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Optional

from feldera import PipelineBuilder
from feldera.pipeline import Pipeline
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import TEST_CLIENT, log
from tests.platform.helper import PipelineTestCase

PIPELINE_SQL = "../docs.feldera.com/docs/use_cases/model_scoring/model_scoring.sql"

# Rows produced by datagen in SQL
SEED_TRANSACTIONS = 6

# Seeded transactions sit one day apart, matching datagen.
DAY_2 = "2024-01-02 00:00:00"
DAY_3 = "2024-01-03 00:00:00"

# The demo controls NOW()
CLOCK_ANCHOR = "2024-01-04T00:00:00Z"

# ---------------------------------------------------------------------------
# The mock model.
# ---------------------------------------------------------------------------

# One logistic regression on the share of the credit limit a transaction takes.
# It fires at 60% or more. It cannot see the merchant category, which is why it
# raises false positives on large purchases that are not fraud.
BIAS, PER_PCT = -1.2, 0.02


def predict(pct_of_limit: float) -> float:
    """Return the fraud probability the model assigns to one feature vector."""
    return 1.0 / (1.0 + math.exp(-(BIAS + PER_PCT * pct_of_limit)))


def sql_timestamp(moment: datetime) -> str:
    """Format an instant the way Feldera parses a TIMESTAMP, in UTC."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class ModelServer(threading.Thread):
    """The model server: the only component that runs the model.

    It reads `unpredicted_features` from the pipeline, and writes to
    `model_prediction`. A production server would probably read a
    Kafka topic rather than an HTTP stream.

    Answering twice upserts on (event_time, trans_id), so re-predicting a
    transaction replaces its prediction.
    """

    def __init__(self, pipeline: Pipeline):
        super().__init__(daemon=True)
        self.pipeline = pipeline
        # Set once the egress stream delivers its first chunk, or `run` fails.
        self._ready = threading.Event()
        # Guards `_requests` while the main thread reads it.
        self._lock = threading.Lock()
        # `trans_id` per request, in arrival order. A history, not a set: a
        # re-prediction may append the same id again. Used for testing only;
        # a production server does not need to keep the history.
        self._requests: list[int] = []
        # The server thread holds this lock while it records a request and
        # writes the prediction, but not while it computes one. `shutdown`
        # acquires it to block until a write in progress has finished.
        self._answering = threading.Lock()
        # True once `shutdown` has asked this thread to stop.
        self._shutting_down = False
        # Whatever ended `run`, raised on the main thread by `shutdown`. A
        # production server reconnects instead; this one stops so the test sees
        # the error.
        self.failure: Optional[BaseException] = None

    def run(self):
        try:
            stream = TEST_CLIENT.listen_to_pipeline(
                self.pipeline.name,
                "unpredicted_features",
                format="json",
                # On connect, replays this view's contents
                send_snapshot=True,
                # Never drop a request
                backpressure=True,
            )
            for chunk in stream():
                self._ready.set()
                if self._shutting_down:
                    break
                self._apply(chunk.get("json_data") or [])
        except BaseException as e:  # re-raised by shutdown() on the main thread
            self.failure = e
            self._ready.set()

    def wait_until_connected(self, timeout_s: float = 60.0):
        assert self._ready.wait(timeout_s), "model server never received a chunk"
        if self.failure is not None:
            raise self.failure

    def shutdown(self, timeout_s: float = 30.0):
        """Start server shutdown.

        Refuse new answers, then wait for the write in flight to finish.

        The server thread then leaves its read loop at the next chunk, dropping
        the subscription.
        """
        # The lock waits for writes in flight
        self._shutting_down = True
        if not self._answering.acquire(timeout=timeout_s):
            raise TimeoutError(f"model server still writing after {timeout_s}s")
        self._answering.release()
        if self.failure is not None:
            raise self.failure

    @property
    def requests(self) -> list[int]:
        """The `trans_id` of each request the model answered, in arrival order."""
        with self._lock:
            return list(self._requests)

    def _apply(self, items: list[Mapping[str, Any]]):
        # Ignore deleted requests
        writes = [self._prediction(i["insert"]) for i in items if "insert" in i]
        if not writes:
            return
        with self._answering:
            if self._shutting_down:
                return
            # Record before writing. `input_json` returns only once
            # the pipeline has processed the batch
            with self._lock:
                self._requests.extend(w["trans_id"] for w in writes)
            self.pipeline.input_json("model_prediction", writes, update_format="raw")

    def _prediction(self, request: Mapping[str, Any]) -> dict:
        probability = predict(float(request["pct_of_limit"]))
        return {
            "event_time": request["ts"],
            "request_fingerprint": request["request_fingerprint"],
            "trans_id": request["trans_id"],
            # Decimals encoded as strings
            "fraud_probability": f"{probability:.4f}",
            "predicted_at": sql_timestamp(datetime.now(timezone.utc)),
        }


class TestModelScoring(PipelineTestCase):
    def setUp(self):
        """Start the demo's own SQL, with NOW() pinned just past the seed data."""
        self.server: Optional[ModelServer] = None
        # Cleanups run last registered first, so this one runs last of all. By
        # then the teardown registered below has stopped the pipeline and closed
        # the stream, so the join returns at once.
        self.addCleanup(self.join_server)

        with open(PIPELINE_SQL, "r") as f:
            sql = f.read()

        self.pipeline = PipelineBuilder(
            TEST_CLIENT,
            name=self.register_for_cleanup("model-scoring"),
            sql=sql,
            runtime_config=RuntimeConfig(
                dev_tweaks={
                    "now_offset": CLOCK_ANCHOR,
                    "now_http_driven": True,
                }
            ),
        ).create_or_replace()
        self.pipeline.start()

    def join_server(self, timeout_s: float = 60.0):
        """Complete the shutdown `ModelServer.shutdown` started.

        Stopping the pipeline ends the stream, which frees the server thread
        from its read. Joining here proves the thread is gone rather than
        assuming a daemon thread will be swept up with the process.
        """
        if self.server is None:
            return
        self.server.join(timeout_s)
        assert not self.server.is_alive(), "model server outlived the pipeline"

    # -- testing code ------------------------------------------------------
    #
    # Everything below until `the lifecycle` exists to verify the demo.

    def scalar(self, query: str) -> Any:
        """Return the first column of the first row. Raises if `query` returns none."""
        row = next(self.pipeline.query(query))
        return next(iter(row.values()))

    def rows(self, query: str) -> list[dict]:
        """Return every row in the result of `query` as a dict."""
        return [dict(row) for row in self.pipeline.query(query)]

    def model_score(self) -> dict:
        """Return the one `model_score` row. An ungrouped aggregate always has one."""
        rows = self.rows("SELECT * FROM model_score")
        assert len(rows) == 1, f"expected exactly one score row: {rows}"
        return rows[0]

    def predicted_fraud(self) -> set[int]:
        """The `trans_id` of every transaction the model currently flags as fraud."""
        return {
            row["trans_id"]
            for row in self.rows(
                "SELECT trans_id FROM predicted_transaction WHERE predicted_fraud"
            )
        }

    # `model_confusion` left-joins labels onto predictions, so its row count is
    # the number of predictions held. COALESCE covers the empty case, where the
    # view has no rows at all.
    STORED_PREDICTIONS = "SELECT COALESCE(SUM(scored), 0) FROM model_score"

    def stored_predictions(self) -> int:
        """How many predictions are held, one per (event_time, trans_id) key."""
        return self.scalar(self.STORED_PREDICTIONS)

    def pending_requests(self) -> int:
        """Requests the pipeline is holding out for the model, asked or not."""
        return self.scalar("SELECT COUNT(*) FROM unpredicted_features")

    def assert_answered(self, answered_count: int, expected: list[int]):
        """Assert `expected` are the only transactions this stage asked for.

        `answered_count` is how long the server's request history was when the
        stage began, so everything past that index belongs to the stage.

        Compared as multisets: chunk arrival order carries no
        meaning, but asking twice for one transaction within a stage does.
        """
        # The stage is over, so nothing may be outstanding: a request still in
        # flight here means the pipeline asked for something unexpected.
        assert self.pending_requests() == 0
        # Note: pending_requests() is blocking, so now we can safely check 'server.requests'.
        asked = self.server.requests[answered_count:]
        assert Counter(asked) == Counter(expected), (
            f"the model was asked for {asked}, not {expected}"
        )

    def wait_for_scalar_query(
        self, query: str, expected: Any, timeout_s: float = 120.0
    ):
        """Wait until a scalar query reaches `expected`."""
        deadline = time.monotonic() + timeout_s
        seen = None
        while time.monotonic() < deadline:
            # A dead model server leaves every prediction barrier unreachable.
            # Report why rather than timing out on the symptom.
            if self.server is not None and self.server.failure is not None:
                raise self.server.failure
            seen = self.scalar(query)
            if seen == expected:
                return
            time.sleep(0.25)
        raise TimeoutError(f"`{query}` stalled at {seen}, expected {expected}")

    # -- the lifecycle ----------------------------------------------------

    def test_model_scoring(self):
        """Run the three scenarios from the module docstring against one pipeline."""
        pipeline = self.pipeline

        # Just the demo using datagen with no model running.
        self.wait_for_scalar_query(
            "SELECT COUNT(*) FROM fingerprinted_features", SEED_TRANSACTIONS
        )
        assert self.stored_predictions() == 0, "the demo alone without a model"

        server = self.server = ModelServer(pipeline)
        server.start()
        # Runs before the cleanup that stops the pipeline, registered in setUp.
        self.addCleanup(server.shutdown)
        server.wait_until_connected()

        # 1. A server that connects after the requests were made still sees all of them.
        self.wait_for_scalar_query(self.STORED_PREDICTIONS, SEED_TRANSACTIONS)
        seeded = server.requests
        log(f"answered {len(seeded)} requests: {seeded}")
        self.assert_answered(0, [1, 2, 3, 4, 5, 6])

        # These values are tied to the actual model prediction formula
        assert self.predicted_fraud() == {2, 3, 5, 6}
        score = self.model_score()
        log(f"score: {score}")
        assert score["scored"] == SEED_TRANSACTIONS
        assert score["true_positive"] == 2
        assert score["false_positive"] == 2
        assert score["false_negative"] == 0
        assert score["precision_score"] == Decimal("0.5")
        assert score["recall_score"] == Decimal("1")

        # 2. Modify a cardholder record by inserting a version dated in the
        #    past. ASOF means only transactions from that date onward see it, so
        #    transaction 1 keeps the limit that was in effect at its own time.
        answered_count = len(seeded)
        pipeline.input_json(
            "cardholder",
            [{"cc_num": 1001, "ts": DAY_2, "zip": 94105, "credit_limit": "20000.00"}],
            update_format="raw",
        )
        # The correction settles once transactions 2 and 3 carry their new prediction:
        # not fraud.
        self.wait_for_scalar_query(
            "SELECT COUNT(*) FROM predicted_transaction WHERE NOT predicted_fraud", 4
        )
        log(f"cardholder correction re-predicted {server.requests[answered_count:]}")

        # Only transactions at or after the new cardholder version.
        self.assert_answered(answered_count, [2, 3])
        assert self.predicted_fraud() == {5, 6}
        # Re-predicting a transaction overwrites the previous prediction
        assert self.stored_predictions() == SEED_TRANSACTIONS
        assert self.scalar("SELECT COUNT(*) FROM predicted_transaction") == (
            SEED_TRANSACTIONS
        )

        corrected = self.model_score()
        log(f"score after correction: {corrected}")
        assert corrected["true_positive"] == 1
        assert corrected["false_positive"] == 1
        assert corrected["false_negative"] == 1

        # 3. The 'is_fraud' label changes months later on a transaction nobody had
        #    labelled. The model score changes.
        answered_count = len(server.requests)
        pipeline.input_json(
            "confirmed_fraud_label",
            [{"trans_id": 3, "ts": DAY_3, "is_fraud": True}],
            update_format="raw",
        )
        self.wait_for_scalar_query("SELECT false_negative FROM model_score", 2)
        # A label revision must not ask the model for anything.
        self.assert_answered(answered_count, [])


if __name__ == "__main__":
    unittest.main()
