import gzip
import io
import json
import time
import zipfile

from feldera.pipeline_builder import PipelineBuilder
from feldera.runtime_config import RuntimeConfig
from feldera.testutils import FELDERA_TEST_NUM_HOSTS, FELDERA_TEST_NUM_WORKERS
from tests import TEST_CLIENT
from .helper import gen_pipeline_name, wait_for_condition

# Time that one call of `slow` takes, so the time of a circuit step that
# processes a record.
STEP_SECS = 45

# Limit on the time to the bundle's first byte. The bundle sends bytes while
# it collects data, so its first byte must not wait for the step.
FIRST_BYTE_SECS = 15


@gen_pipeline_name
def test_support_bundle_starts_during_slow_step(pipeline_name):
    """
    A support bundle that collects new data sends its first bytes at once,
    even while a long circuit step delays the circuit profile. When the
    step is complete, the bundle holds the circuit profile.

    Regression test for feldera/cloud#2163: the bundle sent no byte until it
    had all of its data, so a proxy closed the idle connection.
    """
    sql = """
CREATE TABLE t (x BIGINT NOT NULL);
CREATE FUNCTION slow(x BIGINT NOT NULL) RETURNS BIGINT NOT NULL;
CREATE MATERIALIZED VIEW v AS SELECT slow(x) AS y FROM t;
"""
    udf_rust = f"""
pub fn slow(x: i64) -> Result<i64, Box<dyn std::error::Error>> {{
    std::thread::sleep(std::time::Duration::from_secs({STEP_SECS}));
    Ok(x)
}}
"""
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        sql,
        udf_rust=udf_rust,
        runtime_config=RuntimeConfig(
            workers=FELDERA_TEST_NUM_WORKERS,
            hosts=FELDERA_TEST_NUM_HOSTS,
            fault_tolerance_model=None,
        ),
    ).create_or_replace()
    pipeline.start()

    # Start a slow step.
    pipeline.input_json("t", {"x": 1}, wait=False)
    wait_for_condition(
        "the pipeline receives the record",
        lambda: (pipeline.stats().global_metrics.total_input_records or 0) >= 1,
        timeout_s=60,
        poll_interval_s=0.5,
    )
    # The record is received, but the step that processes it may not have
    # started yet. A bundle requested before that step does not wait for it,
    # so the test would check nothing. The `total_secs` check below detects
    # that case; this sleep makes it unlikely.
    time.sleep(2)

    start = time.monotonic()
    response = TEST_CLIENT.http.get(
        path=f"/pipelines/{pipeline_name}/support_bundle",
        stream=True,
    )
    chunks = response.iter_content(chunk_size=None)
    first = next(chunks)
    first_byte_secs = time.monotonic() - start
    assert first_byte_secs < FIRST_BYTE_SECS, (
        f"first byte after {first_byte_secs:.1f} s"
    )
    assert (pipeline.stats().global_metrics.total_processed_records or 0) == 0, (
        "the step completed before the bundle started, so this test checks nothing"
    )

    bundle = first + b"".join(chunks)
    # The circuit profile waits for the end of the step, so a bundle that
    # started during the step cannot finish before the step does.
    total_secs = time.monotonic() - start
    assert total_secs > STEP_SECS / 2, (
        f"bundle complete after {total_secs:.1f} s, so it did not wait for the step"
    )
    with zipfile.ZipFile(io.BytesIO(bundle)) as zip_file:
        names = zip_file.namelist()
        profiles = [n for n in names if n.endswith("/circuit_profile.json.gz")]
        assert profiles, names
        with zip_file.open(profiles[0]) as member:
            with gzip.GzipFile(fileobj=member) as gz:
                profile = json.load(gz)
    assert "worker_profiles" in profile

    pipeline.stop(force=True)
