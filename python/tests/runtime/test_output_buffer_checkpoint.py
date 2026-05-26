"""
Regression test for issue #6100: a pipeline with enable_output_buffer=true and
max_output_buffer_time_millis=60000 stalls processing for ~60 seconds on every
checkpoint when checkpoint_interval_secs=5.

The root cause: while a checkpoint is in progress, the circuit skips non-barrier
inputs (datagen, ad-hoc queries, etc.), so no records are processed until the
checkpoint completes.  The checkpoint waits for output connectors to finish
transmitting records up to the checkpoint threshold, but with output buffering
enabled the connector holds records in memory until the buffer timeout expires.
The fix lets the pipeline continue processing inputs while the checkpoint runs
in the background.
"""

import time
import uuid

from feldera import PipelineBuilder
from feldera.enums import FaultToleranceModel
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    enterprise_only,
    single_host_only,
)
from tests import TEST_CLIENT

from tests.helper import gen_pipeline_name


@enterprise_only
@single_host_only
@gen_pipeline_name
def test_output_buffer_does_not_stall_checkpoint(pipeline_name):
    """Throughput must not drop to zero while automated checkpointing is active."""
    output_path = f"/tmp/feldera_ob_{uuid.uuid4().hex}.jsonl"

    sql = f"""
CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY)
WITH (
    'connectors' = '[{{
        "transport": {{
            "name": "datagen",
            "config": {{"plan": [{{"rate": 10000}}]}}
        }}
    }}]'
);

CREATE MATERIALIZED VIEW v
WITH (
    'connectors' = '[{{
        "transport": {{
            "name": "file_output",
            "config": {{"path": "{output_path}"}}
        }},
        "format": {{"name": "json"}},
        "enable_output_buffer": true,
        "max_output_buffer_time_millis": 60000
    }}]'
) AS SELECT * FROM t;
""".strip()

    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=FaultToleranceModel.AtLeastOnce,
            checkpoint_interval_secs=5,
        ),
    ).create_or_replace()

    pipeline.start()

    interval_s = 5.0
    num_intervals = 3
    total_s = interval_s * num_intervals

    try:
        # Wait for the pipeline to start processing.
        deadline = time.monotonic() + total_s
        while time.monotonic() < deadline:
            if (pipeline.stats().global_metrics.total_processed_records or 0) > 0:
                break
            time.sleep(0.5)

        # Sample total_processed_records once per checkpoint interval.
        # Checkpoints fire at ~5 s intervals, so at least 2 checkpoints occur.
        # Without the fix, each checkpoint stalls the circuit for ~60 s, and
        # some windows show zero new records.
        samples = [pipeline.stats().global_metrics.total_processed_records or 0]
        for _ in range(num_intervals):
            time.sleep(interval_s)
            samples.append(pipeline.stats().global_metrics.total_processed_records or 0)

        import sys

        print(
            f"\nprocessed_records samples (one per {interval_s}s): {samples}",
            file=sys.stderr,
        )

        for i in range(1, len(samples)):
            delta = samples[i] - samples[i - 1]
            assert delta > 0, (
                f"Throughput dropped to zero in window {i - 1}→{i} "
                f"(processed_records: {samples[i - 1]} → {samples[i]}). "
                f"The output buffer likely stalled the checkpoint (issue #6100)."
            )
    finally:
        pipeline.stop(force=True)
