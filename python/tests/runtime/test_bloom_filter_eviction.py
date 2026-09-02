"""End-to-end check that the false positive rate sheds and restores Bloom modules.

A batch's Bloom filter is written as four modules at the default rate of 1e-4.
Each module is worth one decade of accuracy and 4.81 bits per key, so the rate
configured at load time selects a prefix of them:

    rate     modules   bits/key
    0        0          0        (no filter at all)
    1e-1     1          4.81
    1e-2     2          9.62
    1e-3     3         14.42
    1e-4     4         19.23

This ingests one batch of records at the default rate, checkpoints, and then
reopens that same checkpoint at 1e-4, 1e-2, 0 and 1e-4 again. The files on disk
are never rewritten: they still hold four modules at every step, and only the
number read back changes. Returning to 1e-4 at the end is the point of the last step,
since a filter that had shed modules must come back whole rather than staying
at the accuracy it was last loaded with.

Scale comes from BLOOM_ROW_LIMIT. The ratios do not depend on it, so the default
of ten million rows is enough to separate the steps; a billion behaves identically
and takes about seven minutes to ingest.

Checkpointing is enterprise-only, so this runs only against an enterprise build.
"""

import os

from feldera.pipeline import Pipeline
from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig, Storage
from feldera.testutils import (
    FELDERA_TEST_NUM_HOSTS,
    FELDERA_TEST_NUM_WORKERS,
    log,
    wait_end_of_input,
)
from tests import TEST_CLIENT, enterprise_only
from tests.platform.helper import gen_pipeline_name

ROW_LIMIT = int(os.environ.get("BLOOM_ROW_LIMIT", 10_000_000))

DEFAULT_RATE = 0.0001

# Rates to reopen the one checkpoint at, in order, with the modules each keeps.
# Down from full accuracy, out to no filter, then back to full.
TRANSITIONS = [
    (0.0001, 4),
    (0.01, 2),
    (0.0, 0),
    (0.0001, 4),
]

# The reported size includes a small per-batch struct alongside the module bits,
# so the ratios are not exact.
TOLERANCE = 0.10

# Keys span more than u32 so the table's own batches cannot take the roaring
# path (see `FilterPlan::preferred_filter`); `enable_roaring` is switched off
# below for the streams that still could. The record stays one 8-byte key.
KEY_SPACE = 1 << 40

SQL = """
CREATE TABLE keys (
    id BIGINT NOT NULL PRIMARY KEY
) WITH (
    'materialized' = 'true',
    'connectors' = '[{{
        "transport": {{
            "name": "datagen",
            "config": {{
                "plan": [{{
                    "limit": {limit},
                    "fields": {{ "id": {{ "range": [0, {key_space}] }} }}
                }}]
            }}
        }}
    }}]'
);

CREATE MATERIALIZED VIEW key_count AS SELECT COUNT(*) AS n FROM keys;
"""


def bloom_filter_bytes(pipeline: Pipeline) -> int:
    """Sums resident Bloom filter memory over every operator and worker.

    The circuit profile is the only place `bloom_filter_size_bytes` is
    published; it never reaches `/metrics`.

    # Returns

    Resident Bloom filter memory, in bytes.
    """
    profile = pipeline.client.http.get(
        path=f"/pipelines/{pipeline.name}/circuit_json_profile"
    )
    total = 0
    found = False
    for worker in profile.get("worker_profiles", []):
        for readings in worker.get("metadata", {}).values():
            for reading in readings:
                if reading.get("metric_id") == "bloom_filter_size_bytes":
                    total += int(reading["value"]["value"])
                    found = True
    assert found, "the circuit profile carries no bloom_filter_size_bytes reading"
    return total


def storage_config(rate: float) -> Storage:
    """Storage with `rate`, and every batch on storage so filters exist at all."""
    return Storage(
        config={"bloom_false_positive_rate": rate},
        min_storage_bytes=0,
    )


def runtime_config(rate: float) -> RuntimeConfig:
    return RuntimeConfig(
        workers=FELDERA_TEST_NUM_WORKERS,
        hosts=FELDERA_TEST_NUM_HOSTS,
        provisioning_timeout_secs=300,
        storage=storage_config(rate),
        # Make sure roaring bitmaps don't kick in instead of Bloom filters.
        dev_tweaks={"enable_roaring": False},
    )


def measure_at_rate(pipeline: Pipeline, rate: float) -> tuple[int, int]:
    """Reopens the checkpoint at `rate` and measures the filters it loads.

    Starts paused so the datagen connector cannot run again: this pipeline is
    not fault tolerant, so a running connector would re-ingest from scratch and
    the filters would no longer describe the checkpointed batches.
    """
    config = pipeline.runtime_config()
    config.storage = storage_config(rate).__dict__
    pipeline.set_runtime_config(config)

    pipeline.start_paused()
    try:
        # Asks the restored state itself rather than an ingest counter, so this
        # holds whatever a restart does to the counters. It also covers the
        # no-filter step, where every lookup has to fall through to storage.
        rows = next(iter(pipeline.query("SELECT n FROM key_count")))["n"]
        assert rows == ROW_LIMIT, (
            f"rate {rate:g}: checkpoint restored {rows} rows, expected {ROW_LIMIT}"
        )
        return bloom_filter_bytes(pipeline), rows
    finally:
        # Forced, so this run leaves no new checkpoint and the next rate reads
        # the same batches this one did.
        pipeline.stop(force=True)


@enterprise_only
@gen_pipeline_name
def test_bloom_filter_eviction_follows_the_rate(pipeline_name: str) -> None:
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql=SQL.format(limit=ROW_LIMIT, key_space=KEY_SPACE),
        runtime_config=runtime_config(DEFAULT_RATE),
    ).create_or_replace()

    try:
        log(f"ingesting {ROW_LIMIT} records at the default rate {DEFAULT_RATE:g}")
        pipeline.start()
        wait_end_of_input(pipeline)
        # Checkpoints before stopping, which is the checkpoint every step below
        # reopens.
        pipeline.stop(force=False)

        measured = []
        for rate, modules in TRANSITIONS:
            filter_bytes, rows = measure_at_rate(pipeline, rate)
            measured.append(filter_bytes)
            log(
                f"rate {rate:g}: {modules} module(s), "
                f"{filter_bytes / (1 << 20):.1f} MiB of filter over {rows} rows "
                f"({filter_bytes * 8 / rows:.2f} bits/key)"
            )

        full, halved, none, restored = measured

        # A filter that never reached storage still reports a handful of bytes
        # per operator, and the ratios below would then compare noise against
        # noise. Four modules cost 19.23 bits per key, so anything near zero
        # means the Bloom path was not exercised at all. There is no upper
        # bound: this sums every spine holding these keys, and how many of those
        # the plan builds is not this test's business.
        bits_per_key = full * 8 / ROW_LIMIT
        assert bits_per_key >= 15.0, (
            f"four modules measured {bits_per_key:.2f} bits/key, expected at "
            "least the 19.23 one filter costs; the keys are probably not behind "
            "Bloom filters at all"
        )

        # 1e-4 -> 1e-2 sheds half the modules.
        ratio = halved / (full / 2)
        assert abs(ratio - 1.0) <= TOLERANCE, (
            f"1e-2 kept {halved} bytes, expected about {full / 2:.0f}, half of "
            f"the {full} that 1e-4 kept ({ratio:.2f}x off)"
        )

        # 1e-2 -> 0 drops the filter outright rather than shrinking it.
        assert none == 0, f"a rate of 0 left {none} bytes of Bloom filter resident"

        # 0 -> 1e-4 comes back whole: the files still hold four modules, and
        # residency is decided afresh on every load. Compared within a tolerance
        # rather than exactly, because a batch rewritten between the two loads
        # carries a filter written at whatever rate was in force then.
        ratio = restored / full
        assert abs(ratio - 1.0) <= TOLERANCE, (
            f"returning to 1e-4 gave {restored} bytes against the {full} it "
            f"started with ({ratio:.2f}x); the filter did not come back whole"
        )
    finally:
        pipeline.stop(force=True)
        # Storage outlives a stopped pipeline, and delete refuses while it is
        # still there.
        pipeline.clear_storage()
        pipeline.delete()
