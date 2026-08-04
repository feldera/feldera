"""
Integration test for the `clock_timezone_offset` pipeline property.

The pipeline is configured with `clock_timezone_offset = "+05:30"` plus the
deterministic clock dev tweaks (`now_offset` anchor, `now_http_driven`), so
every asserted `NOW()` value is an exact constant.  The test exercises:

* `NOW()` returns the anchor shifted by the timezone offset,
* advances compound on the shifted value,
* the shifted value is visible to SQL via an adhoc query against a
  materialized view of `SELECT NOW()`,
* editing `clock_timezone_offset` is rejected while the pipeline's storage
  is in use (the offset is baked into checkpointed state),
* re-submitting the unchanged offset is accepted,
* the offset is editable again once storage is cleared.
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

# `2030-01-01T00:00:00Z` in milliseconds since epoch.  A fixed anchor, so
# every assertion in this test can use literal values.
ANCHOR_RFC = "2030-01-01T00:00:00Z"
ANCHOR_MS = 1_893_456_000_000

ONE_MINUTE_MS = 60_000

TZ_OFFSET = "+05:30"
TZ_OFFSET_MS = (5 * 60 + 30) * ONE_MINUTE_MS

# Clock resolution configured below; `advance()` values round to this.
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


class TestClockTimezoneOffset(unittest.TestCase):
    def test_offset_applied_and_pinned(self):
        pipeline_name = unique_pipeline_name("test_clock_timezone_offset")

        sql = "CREATE MATERIALIZED VIEW v AS SELECT NOW() AS t;"

        pipeline = PipelineBuilder(
            TEST_CLIENT,
            pipeline_name,
            sql=sql,
            runtime_config=RuntimeConfig(
                workers=FELDERA_TEST_NUM_WORKERS,
                hosts=FELDERA_TEST_NUM_HOSTS,
                clock_resolution_usecs=CLOCK_RESOLUTION_MS * 1_000,
                clock_timezone_offset=TZ_OFFSET,
                dev_tweaks={
                    "now_offset": ANCHOR_RFC,
                    "now_http_driven": True,
                },
            ),
        ).create_or_replace()

        try:
            pipeline.start()
            try:
                self.assertEqual(pipeline.status(), PipelineStatus.RUNNING)

                # NOW() is the anchor shifted by the timezone offset.
                resp = _advance_and_settle(pipeline, 0)
                self.assertEqual(resp["now_ms"], ANCHOR_MS + TZ_OFFSET_MS)
                view_now = _view_now(pipeline)
                self.assertTrue(
                    view_now.startswith("2030-01-01T05:30:00"),
                    f"view NOW() is {view_now!r}, expected 2030-01-01T05:30:00 "
                    f"(anchor {ANCHOR_RFC} shifted by {TZ_OFFSET})",
                )

                # Advances compound on the shifted value.
                resp = _advance_and_settle(pipeline, ONE_MINUTE_MS)
                self.assertEqual(
                    resp["now_ms"], ANCHOR_MS + TZ_OFFSET_MS + ONE_MINUTE_MS
                )
                view_now = _view_now(pipeline)
                self.assertTrue(
                    view_now.startswith("2030-01-01T05:31:00"),
                    f"view NOW() is {view_now!r}, expected 2030-01-01T05:31:00",
                )
            finally:
                pipeline.stop(force=True)

            # Stopped, but storage is still in use: changing the offset is
            # rejected because it is baked into checkpointed state.
            runtime_cfg = pipeline.runtime_config()
            runtime_cfg.clock_timezone_offset = "-08:00"
            with self.assertRaises(FelderaAPIError) as cm:
                pipeline.set_runtime_config(runtime_cfg)
            self.assertEqual(cm.exception.status_code, 400)
            self.assertIn("clock_timezone_offset", cm.exception.message or "")

            # Removing the offset is a change, too.
            runtime_cfg.clock_timezone_offset = None
            with self.assertRaises(FelderaAPIError) as cm:
                pipeline.set_runtime_config(runtime_cfg)
            self.assertEqual(cm.exception.status_code, 400)

            # Re-submitting the unchanged offset is not a change.
            runtime_cfg.clock_timezone_offset = TZ_OFFSET
            pipeline.set_runtime_config(runtime_cfg)
            self.assertEqual(pipeline.runtime_config().clock_timezone_offset, TZ_OFFSET)
        finally:
            pipeline.clear_storage()

        # With storage cleared the offset is editable again.
        runtime_cfg = pipeline.runtime_config()
        runtime_cfg.clock_timezone_offset = "-08:00"
        pipeline.set_runtime_config(runtime_cfg)
        self.assertEqual(pipeline.runtime_config().clock_timezone_offset, "-08:00")


if __name__ == "__main__":
    unittest.main()
