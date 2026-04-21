from build.lib.feldera.pipeline_builder import PipelineBuilder
from feldera.enums import ProgramStatus
from tests import TEST_CLIENT
from tests.platform.helper import gen_pipeline_name


@gen_pipeline_name
def test_pipeline_wait_for_compilation(pipeline_name):
    """
    Tests the waiting of a pipeline to complete its compilation.
    """

    # Successful compilation (I)
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql="",
    ).create_or_replace(wait=False)
    pipeline.wait_for_compilation()
    assert pipeline.program_status() == ProgramStatus.Success

    # Failed waiting due to too short timeout.
    # Compilation takes several seconds at minimum, so a single-second
    # timeout should be sufficient for this test to be deterministic.
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql="CREATE TABLE t1 (i1 INT);",
    ).create_or_replace(wait=False)
    e = None
    try:
        pipeline.wait_for_compilation(timeout_s=1.0)
    except TimeoutError as exc:
        e = exc
    assert e is not None

    # Failed due to SQL error
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql="invalid-sql",
    ).create_or_replace(wait=False)
    e = None
    try:
        pipeline.wait_for_compilation()
    except RuntimeError as exc:
        e = exc
    assert e is not None
    assert pipeline.program_status() == ProgramStatus.SqlError

    # Failed due to Rust error
    pipeline = PipelineBuilder(
        TEST_CLIENT, name=pipeline_name, sql="", udf_rust="invalid-rust"
    ).create_or_replace(wait=False)
    e = None
    try:
        pipeline.wait_for_compilation()
    except RuntimeError as exc:
        e = exc
    assert e is not None
    assert pipeline.program_status() == ProgramStatus.RustError

    # Successful compilation (II)
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql="CREATE TABLE t2 (i2 INT);",
    ).create_or_replace(wait=False)
    pipeline.wait_for_compilation()
    assert pipeline.program_status() == ProgramStatus.Success
