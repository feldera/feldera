import time
import unittest
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

        # Process more clock ticks
        pipeline.resume()
        time.sleep(5)

        validate_outputs(pipeline, tables, views)

        assert next(pipeline.query("select count(*) as cnt from v_now"))["cnt"] == 1

        pipeline.stop(force=True)


if __name__ == "__main__":
    unittest.main()
