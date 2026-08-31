"""Tests the model-scoring use case.

Runs an ML model as a separate process outside the pipeline. The two exchange data only
through the a view (to model) and one input table (from model).

    1. predict on the seed data
    2. correct a cardholder record, re-predicting only the affected window
    3. a late fraud label arrives, moving the score without any new prediction

The SQL is read from the use-case directory, so the two cannot drift.
"""

import math
import threading
import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Optional

from feldera import PipelineBuilder
from feldera.pipeline import Pipeline
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import TEST_CLIENT, log
from tests.platform.helper import PipelineTestCase

PIPELINE_SQL = "../docs.feldera.com/docs/use_cases/model_scoring/model_scoring.sql"

# Rows the demo's datagen connectors seed.
SEED_TRANSACTIONS = 6

# Seeded transactions sit one day apart, matching the demo's datagen plan.
DAY_1 = "2024-01-01 00:00:00"
DAY_2 = "2024-01-02 00:00:00"
DAY_3 = "2024-01-03 00:00:00"

# The demo filters on NOW(), so the test anchors NOW() just after the seeded
# transactions and moves it only when it wants to.
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
    `model_prediction`.
    """

    def __init__(self, pipeline: Pipeline):
        super().__init__(daemon=True)
        self.pipeline = pipeline
        self._ready = threading.Event()
        # Transactions the model answered a request for, in answer order.
        self.answered: list[int] = []
        # Answers the pipeline has confirmed processing. This trails `answered`:
        # `input_json` returns only once a completion poll observes the batch,
        # which can be a poll interval after the pipeline processed it. Wait on
        # this counter to synchronize with the pipeline; read `answered` to count
        # the times the model ran.
        self.answers_processed = 0
        self.failure: Optional[BaseException] = None

    def run(self):
        try:
            stream = TEST_CLIENT.listen_to_pipeline(
                self.pipeline.name,
                "unpredicted_features",
                format="json",
                # On connect, replays this view's contents: the requests still
                # unanswered, not the whole 30-day window.
                send_snapshot=True,
                # Never drop a request
                backpressure=True,
            )
            for chunk in stream():
                self._ready.set()
                self._apply(chunk.get("json_data") or [])
        except BaseException as e:  # re-raised by stop() on the main thread
            self.failure = e
            self._ready.set()

    def wait_until_connected(self, timeout_s: float = 60.0):
        assert self._ready.wait(timeout_s), "model server never received a chunk"
        if self.failure is not None:
            raise self.failure

    def stop(self):
        if self.failure is not None:
            raise self.failure

    def _apply(self, items: list[Mapping[str, Any]]):
        # Ignore deleted requests
        writes = [self._prediction(i["insert"]) for i in items if "insert" in i]
        if writes:
            # `input_json` sits between the two updates on purpose: its return is
            # the only point at which the pipeline has confirmed these answers.
            self.answered += [write["trans_id"] for write in writes]
            self.pipeline.input_json("model_prediction", writes, update_format="raw")
            self.answers_processed += len(writes)

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

    # -- testing code ------------------------------------------------------
    #
    # Everything below until `the lifecycle` exists to verify the demo.

    def scalar(self, query: str) -> Any:
        row = next(self.pipeline.query(query))
        return next(iter(row.values()))

    def rows(self, query: str) -> list[dict]:
        return [dict(row) for row in self.pipeline.query(query)]

    def model_score(self) -> dict:
        rows = self.rows("SELECT * FROM model_score")
        assert len(rows) == 1, f"expected exactly one score row: {rows}"
        return rows[0]

    def predicted_fraud(self) -> set[int]:
        return {
            row["trans_id"]
            for row in self.rows(
                "SELECT trans_id FROM predicted_transaction WHERE predicted_fraud"
            )
        }

    def stored_predictions(self) -> int:
        # `model_confusion` left-joins labels onto predictions, so its row count
        # is the number of predictions held. COALESCE covers the empty case,
        # where the view has no rows at all.
        return self.scalar("SELECT COALESCE(SUM(scored), 0) FROM model_score")

    def wait_until(self, predicate, description: str, timeout_s: float = 120.0):
        """Wait for a condition the pipeline cannot report, e.g. server counters."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.25)
        raise TimeoutError(f"timed out waiting for {description}")

    def wait_for(self, query: str, expected: Any, timeout_s: float = 120.0):
        """Wait until a scalar query reaches `expected`."""
        deadline = time.monotonic() + timeout_s
        seen = None
        while time.monotonic() < deadline:
            seen = self.scalar(query)
            if seen == expected:
                return
            time.sleep(0.25)
        raise TimeoutError(f"`{query}` stalled at {seen}, expected {expected}")

    # -- the lifecycle ----------------------------------------------------

    def test_model_scoring(self):
        pipeline = self.pipeline

        # Just the demo using datagen with no model running.
        self.wait_for("SELECT COUNT(*) FROM fingerprinted_features", SEED_TRANSACTIONS)
        assert self.stored_predictions() == 0, "the demo alone runs no model"

        server = ModelServer(pipeline)
        server.start()
        self.addCleanup(server.stop)
        server.wait_until_connected()

        # 1. A server that connects after the requests were made still sees all
        #    of them. Wait on the server, not on a view: an ad-hoc query can see a
        #    prediction before the server's own `input_json` returns, and a seed
        #    answer confirmed after this point would land in step 2's slice.
        self.wait_until(
            lambda: server.answers_processed >= SEED_TRANSACTIONS,
            "the seeded transactions to be predicted",
        )
        log(f"answered requests for transactions {sorted(server.answered)}")

        assert sorted(server.answered) == list(range(1, SEED_TRANSACTIONS + 1)), (
            "one request per seeded transaction"
        )
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
        before = len(server.answered)
        pipeline.input_json(
            "cardholder",
            [{"cc_num": 1001, "ts": DAY_2, "zip": 94105, "credit_limit": "20000.00"}],
            update_format="raw",
        )
        # Wait for the two re-predictions to be processed. `scored` cannot serve
        # as the barrier here: a re-prediction upserts on (event_time, trans_id),
        # so it stays at SEED_TRANSACTIONS throughout. Step 1 left
        # `answers_processed` at `before`, so the seed answers are all counted.
        self.wait_until(
            lambda: server.answers_processed >= before + 2,
            "the corrected transactions to be re-predicted",
        )
        repredicted = sorted(server.answered[before:])
        log(f"cardholder correction re-predicted transactions {repredicted}")

        assert repredicted == [2, 3], "only transactions at or after the new version"
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
        #    labelled. Labels are not model inputs, so the model score changes without
        #    asking the model for anything.
        answered_before = list(server.answered)
        assert self.model_score()["false_negative"] == 1
        pipeline.input_json(
            "confirmed_fraud_label",
            [{"trans_id": 3, "ts": DAY_3, "is_fraud": True}],
            update_format="raw",
        )
        self.wait_for("SELECT false_negative FROM model_score", 2)
        assert server.answered == answered_before, (
            "a label revision must not ask the model for anything"
        )


if __name__ == "__main__":
    unittest.main()
