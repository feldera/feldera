import time
import unittest

from feldera.enums import PipelineStatus
from feldera.pipeline import Pipeline
from feldera.runtime_config import Resources
from feldera.testutils import (
    ViewSpec,
    build_pipeline,
    log,
    validate_outputs,
    unique_pipeline_name,
)

# Add now() value to a table in a transaction.

INPUT_RECORDS = 2000000

# After restart, the implicit `now` clock connector uses this resolution (see
# `now_endpoint_config` in the adapters crate). Stats only expose `stream` for
# the endpoint; the effective resolution is the pipeline runtime setting.
CLOCK_RESOLUTION_USECS_AFTER_RESTART = 3_000_000


def _clock_input_endpoint(pipeline: Pipeline):
    """Return stats for the built-in real-time clock input (`endpoint_name` ``now``)."""
    for inp in pipeline.stats().inputs:
        if inp.endpoint_name == "now":
            return inp
    names = [i.endpoint_name for i in pipeline.stats().inputs]
    raise AssertionError(f"clock input endpoint 'now' not found; have {names!r}")


def _wait_clock_record_ticks(
    pipeline: Pipeline, count: int, *, timeout_s: float
) -> None:
    """Wait until the clock connector's ingested record count increases by ``count``."""
    baseline = _clock_input_endpoint(pipeline).metrics.total_records
    assert baseline is not None
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        cur = _clock_input_endpoint(pipeline).metrics.total_records
        if cur is not None and cur >= baseline + count:
            return
        time.sleep(0.25)
    raise TimeoutError(
        f"clock total_records did not increase by {count} within {timeout_s}s "
        f"(baseline was {baseline})"
    )


class TestNow(unittest.TestCase):
    def test_now(self):
        tables = {
            "t": f"""
            create table t(
                id bigint not null primary key,
                company_id bigint,
                name string
            ) with (
            'materialized' = 'true',
            'connectors' = '[{{
                "name": "datagen",
                "transport": {{
                    "name": "datagen",
                    "config": {{
                        "plan": [{{
                            "limit": {INPUT_RECORDS},
                            "fields": {{
                                "name": {{ "strategy": "sentence" }}
                            }}
                        }}]
                    }}
                }}
            }}]');
            """
        }

        views = [
            ViewSpec(
                # Have a deterministic now() for testing against datafusion.
                "v_now",
                """
                select
                    now() as now_ts
                """,
            ),
            ViewSpec(
                "v",
                """
                select
                    t.*,
                    v_now.now_ts as now_ts
                from t, v_now
                """,
            ),
        ]

        pipeline = build_pipeline(
            unique_pipeline_name("now-test"),
            tables,
            views,
            # This test uses >2GB of storage in the ad hoc query.
            resources=Resources(storage_mb_max=8192),
        )

        pipeline.start()
        start_time = time.monotonic()

        pipeline.start_transaction()

        while (
            pipeline.stats().global_metrics.total_input_records < INPUT_RECORDS
            or pipeline.stats().global_metrics.buffered_input_records > 0
        ):
            log(f"Waiting for {INPUT_RECORDS} records to be ingested...")
            time.sleep(1)

        elapsed = time.monotonic() - start_time
        log(f"Data ingested in {elapsed}")

        # Freeze the value of now().
        pipeline.pause()
        pipeline.wait_for_idle()

        start_time = time.monotonic()
        pipeline.commit_transaction(transaction_id=None, wait=True, timeout_s=600)

        elapsed = time.monotonic() - start_time
        log(f"Commit took {elapsed}")

        # Don't validate v_now which depends on the real-time clock.
        views = [view for view in views if view.name != "v_now"]
        validate_outputs(pipeline, tables, views)

        # Tighten clock resolution, restart, confirm runtime and clock endpoint,
        # then wait for two more clock ticks before validating again.
        pipeline.stop(force=False)
        runtime_cfg = pipeline.runtime_config()
        runtime_cfg.clock_resolution_usecs = CLOCK_RESOLUTION_USECS_AFTER_RESTART
        pipeline.set_runtime_config(runtime_cfg)
        pipeline.start()
        pipeline.wait_for_status(PipelineStatus.RUNNING, timeout=300)

        assert (
            pipeline.runtime_config().clock_resolution_usecs
            == CLOCK_RESOLUTION_USECS_AFTER_RESTART
        )
        clock_status = _clock_input_endpoint(pipeline)
        assert clock_status.config is not None
        assert clock_status.config.get("stream") == "now"

        # Two ticks at 3s resolution need at least ~6s; allow slack for scheduling.
        _wait_clock_record_ticks(pipeline, 2, timeout_s=30.0)

        validate_outputs(pipeline, tables, views)

        assert next(pipeline.query("select count(*) as cnt from v_now"))["cnt"] == 1

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
