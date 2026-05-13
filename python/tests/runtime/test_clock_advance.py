"""
Integration test for `POST /v0/pipelines/{name}/clock/advance`.

The pipeline is configured with both `dev_tweaks.now_offset` (the anchor)
and `dev_tweaks.now_http_driven = true` (the externally-driven mode).
The test exercises:

* the read-only `advance(0)`,
* explicit forward advances and compounding,
* `advance(null)`: advance by one `clock_resolution`,
* server-side rejection of negative deltas,
* visibility of the new `NOW()` to SQL via an adhoc query against a
  materialized view of `SELECT NOW()`.

Every value is asserted exactly; the test does not depend on wall clock.
"""

import unittest

from feldera.enums import PipelineStatus
from feldera.pipeline_builder import PipelineBuilder
from feldera.rest.errors import FelderaAPIError
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    unique_pipeline_name,
)
from tests import TEST_CLIENT

# `2030-01-01T00:00:00Z` in milliseconds since epoch.  Chosen as a fixed
# point in time so every assertion in this test can use literal values.
ANCHOR_RFC = "2030-01-01T00:00:00Z"
ANCHOR_MS = 1_893_456_000_000

ONE_MINUTE_MS = 60_000
ONE_DAY_MS = 24 * 60 * 60 * 1_000

# Clock resolution configured below; `advance(null)` moves NOW() by this much.
CLOCK_RESOLUTION_MS = 1_000


def _advance_and_settle(pipeline, delta_ms: int | None) -> dict:
    """`advance_clock(delta_ms)` followed by `wait_for_idle` so the view sees the new tick."""
    resp = pipeline.advance_clock(delta_ms)
    pipeline.wait_for_idle(idle_interval_s=0.5, timeout_s=10.0, poll_interval_s=0.05)
    return resp


def _view_now(pipeline) -> str:
    """The current `NOW()` value visible in the materialized view."""
    rows = list(pipeline.query("SELECT t FROM v;"))
    assert rows, "materialized view `v` has no rows"
    return str(rows[0]["t"])


class TestClockAdvance(unittest.TestCase):
    @unittest.skip(
        "POST /clock/advance is new in this PR; re-enable once the platform CI is updated."
    )
    def test_anchor_plus_advance(self):
        pipeline_name = unique_pipeline_name("test_clock_advance")

        sql = "CREATE MATERIALIZED VIEW v AS SELECT NOW() AS t;"

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                # 1-second resolution; advance() values are rounded to this
                # and `advance(null)` moves NOW() by exactly this much.
                clock_resolution_usecs=CLOCK_RESOLUTION_MS * 1_000,
                dev_tweaks={
                    "now_offset": ANCHOR_RFC,
                    "now_http_driven": True,
                },
            ),
        ).create_or_replace()

        pipeline.start()
        try:
            self.assertEqual(pipeline.status(), PipelineStatus.RUNNING)

            # advance(0) is a read: returns the anchor without moving NOW().
            resp = _advance_and_settle(pipeline, 0)
            self.assertEqual(resp["now_ms"], ANCHOR_MS)
            self.assertTrue(
                resp["now"].startswith("2030-01-01T00:00:00"),
                f"unexpected RFC 3339: {resp['now']!r}",
            )

            # advance(null) advances by exactly one clock_resolution.
            resp = _advance_and_settle(pipeline, None)
            self.assertEqual(resp["now_ms"], ANCHOR_MS + CLOCK_RESOLUTION_MS)

            # advance(+1min) moves NOW() forward; the materialized view
            # must reflect the same value once the step has settled.
            resp = _advance_and_settle(pipeline, ONE_MINUTE_MS)
            self.assertEqual(
                resp["now_ms"], ANCHOR_MS + CLOCK_RESOLUTION_MS + ONE_MINUTE_MS
            )
            self.assertTrue(_view_now(pipeline).startswith("2030-01-01T00:01:01"))

            # advance(+1 day) compounds with the previous steps.
            resp = _advance_and_settle(pipeline, ONE_DAY_MS)
            self.assertEqual(
                resp["now_ms"],
                ANCHOR_MS + CLOCK_RESOLUTION_MS + ONE_MINUTE_MS + ONE_DAY_MS,
            )
            self.assertTrue(_view_now(pipeline).startswith("2030-01-02T00:01:01"))

            # advance(0) confirms the clock did not drift between calls.
            resp = _advance_and_settle(pipeline, 0)
            self.assertEqual(
                resp["now_ms"],
                ANCHOR_MS + CLOCK_RESOLUTION_MS + ONE_MINUTE_MS + ONE_DAY_MS,
            )

            # Negative deltas are rejected by the server (the request body
            # field is `u64`, so JSON deserialization fails with 400).
            with self.assertRaises(FelderaAPIError) as cm:
                pipeline.advance_clock(-1)
            self.assertEqual(cm.exception.status_code, 400)

            resp = _advance_and_settle(pipeline, 0)
            self.assertEqual(
                resp["now_ms"],
                ANCHOR_MS + CLOCK_RESOLUTION_MS + ONE_MINUTE_MS + ONE_DAY_MS,
                "NOW() must not change after a rejected advance",
            )
        finally:
            pipeline.stop(force=True)
            pipeline.clear_storage()

    @unittest.skip(
        "POST /clock/advance is new in this PR; re-enable once the platform CI is updated."
    )
    def test_pre_epoch_anchor(self):
        """Pre-1970 anchor: `now_ms` is negative end-to-end."""
        pipeline_name = unique_pipeline_name("test_pre_epoch_clock")

        # 1950-01-01T00:00:00Z in epoch ms.
        pre_epoch_rfc = "1950-01-01T00:00:00Z"
        pre_epoch_ms = -631_152_000_000

        sql = "CREATE MATERIALIZED VIEW v AS SELECT NOW() AS t;"

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                clock_resolution_usecs=CLOCK_RESOLUTION_MS * 1_000,
                dev_tweaks={
                    "now_offset": pre_epoch_rfc,
                    "now_http_driven": True,
                },
            ),
        ).create_or_replace()

        pipeline.start()
        try:
            self.assertEqual(pipeline.status(), PipelineStatus.RUNNING)

            # advance(0): the anchor reads back as a negative now_ms.
            resp = _advance_and_settle(pipeline, 0)
            self.assertEqual(resp["now_ms"], pre_epoch_ms)
            self.assertLess(resp["now_ms"], 0)
            self.assertTrue(
                resp["now"].startswith("1950-01-01T00:00:00"),
                f"unexpected RFC 3339: {resp['now']!r}",
            )

            # Forward by one day: still pre-1970, still negative.
            resp = _advance_and_settle(pipeline, ONE_DAY_MS)
            self.assertEqual(resp["now_ms"], pre_epoch_ms + ONE_DAY_MS)
            self.assertLess(resp["now_ms"], 0)
            self.assertTrue(
                resp["now"].startswith("1950-01-02T00:00:00"),
                f"unexpected RFC 3339: {resp['now']!r}",
            )
        finally:
            pipeline.stop(force=True)
            pipeline.clear_storage()


if __name__ == "__main__":
    unittest.main()
