"""
Test for the default value of ``max_output_buffer_size_records``.

Output buffering decouples the rate at which the pipeline produces changes from
the rate at which they are pushed to an output connector.  The buffer is flushed
when either of two thresholds is crossed: it has held data for longer than
``max_output_buffer_time_millis`` or it has accumulated more than
``max_output_buffer_size_records`` records.

``max_output_buffer_size_records`` defaults to 10,000,000 records, which bounds
the buffer and guarantees it is sent once it grows past that size even when no
time limit is configured.

This test enables output buffering on a Delta Lake sink without setting a time
limit, feeds it more than 10,000,000 records, and verifies that records are
written out: the pipeline's completed-record count advances past the default
size cap.
"""

import json

from feldera import Pipeline, PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from tests.utils import DeltaTestLocation, wait_for_condition

from .helper import gen_pipeline_name

# Default value of ``max_output_buffer_size_records``.  The buffer must flush
# once it grows past this many records, even without a time limit.
_DEFAULT_MAX_OUTPUT_BUFFER_SIZE_RECORDS = 10_000_000

# Number of records to generate.  Chosen comfortably above the default size cap
# so that the buffer is forced to flush at least once.
_NUM_RECORDS = 12_000_000


@gen_pipeline_name
def test_output_buffer_flushes_at_default_size_limit(pipeline_name):
    """A buffered Delta sink with no time limit still flushes at 10M records."""

    location = DeltaTestLocation.create(pipeline_name)

    # Use 8 writer threads so the Delta sink keeps up with the large flush.
    delta_config = dict(location.connector_config)
    delta_config["threads"] = 8

    delta_connector = {
        "name": "delta_out",
        "transport": {
            "name": "delta_table_output",
            "config": delta_config,
        },
        # The Delta sink needs an index on the view to write with threads > 1.
        "index": "v_idx",
        # Enable buffering but set neither ``max_output_buffer_time_millis``
        # nor ``max_output_buffer_size_records``, so the buffer relies on the
        # default 10M size cap to flush.
        "enable_output_buffer": True,
    }

    # Generate incrementing primary keys so that every record is distinct and
    # the buffer cannot shrink by consolidating updates to the same key.
    datagen_connector = {
        "transport": {
            "name": "datagen",
            "config": {
                "workers": 4,
                "plan": [
                    {
                        "limit": _NUM_RECORDS,
                        "fields": {"id": {"range": [0, _NUM_RECORDS]}},
                    }
                ],
            },
        }
    }

    sql = f"""
CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY) WITH (
    'connectors' = '{json.dumps([datagen_connector])}'
);

CREATE MATERIALIZED VIEW v WITH (
    'connectors' = '{json.dumps([delta_connector])}'
) AS SELECT * FROM t;

CREATE INDEX v_idx ON v(id);
""".strip()

    pipeline: Pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql=sql,
        runtime_config=RuntimeConfig(workers=FELDERA_TEST_NUM_WORKERS),
    ).create_or_replace()

    pipeline.start()

    try:
        # The buffer flushes once it crosses the 10M default size cap, pushing
        # those records through the Delta sink and advancing the completed
        # count past the cap.
        wait_for_condition(
            "completed-record count advances past the default buffer size cap",
            lambda: (
                (pipeline.stats().global_metrics.total_completed_records or 0)
                >= _DEFAULT_MAX_OUTPUT_BUFFER_SIZE_RECORDS
            ),
            timeout_s=600.0,
            poll_interval_s=2.0,
        )
    finally:
        pipeline.stop(force=True)
