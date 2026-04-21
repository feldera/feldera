import time
import pytest
from typing import Generator
from requests.exceptions import ChunkedEncodingError

from feldera import Pipeline, PipelineBuilder
from feldera.rest.errors import FelderaAPIError
from tests import TEST_CLIENT
from tests.platform.helper import gen_pipeline_name


def validate_tracking(
    pipeline_name: str, current: dict, tracking: Generator[dict, None, None]
):
    expectation = TEST_CLIENT.http.get(f"/pipelines/{pipeline_name}?selector=all")
    start_s = time.monotonic()
    for delta in tracking:
        current.update(delta)
        if time.monotonic() - start_s >= 1.0:
            break
    # If the assertion below fails, it will provide feedback on what is the difference.
    # It might be in the future that the moments at which this validation function is called
    # are stable (as in, the pipeline fields shouldn't change) might not be in the future
    # if time-dependent content like timestamps is added (e.g., particularly the `*_details` fields).
    # In this case, it will be necessary to either exclude fields from comparison, or to
    # nullify them before the comparison.
    assert current == expectation


@gen_pipeline_name
def test_pipeline_track(pipeline_name):
    """
    Tests that the pipeline tracking functionality yields the same result as the direct
    GET of the pipeline.
    """

    # Create the pipeline and immediately start tracking it
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        name=pipeline_name,
        sql="",
    ).create_or_replace(wait=False)
    tracking = pipeline.track()

    # Wait for compilation to complete
    pipeline.wait_for_compilation()

    # At a moment where the state is stable, compare the updated tracked version with
    # direct GET of the pipeline
    current = {}
    validate_tracking(pipeline_name, current, tracking)
    pipeline.start()
    validate_tracking(pipeline_name, current, tracking)
    pipeline.pause()
    validate_tracking(pipeline_name, current, tracking)
    pipeline.resume()
    validate_tracking(pipeline_name, current, tracking)
    pipeline.stop(force=False, wait=False)
    pipeline.stop(force=True, wait=True)
    validate_tracking(pipeline_name, current, tracking)
    pipeline.clear_storage()
    validate_tracking(pipeline_name, current, tracking)

    # Edit description (I)
    new_description = "description-1"
    TEST_CLIENT.http.patch(
        f"/pipelines/{pipeline_name}", {"description": new_description}
    )
    assert pipeline.description() == new_description
    validate_tracking(pipeline_name, current, tracking)

    # Renaming pipeline should not interrupt tracking
    new_name = f"{pipeline_name}-renamed"
    # Delete renamed one if present
    try:
        TEST_CLIENT.http.delete(f"/pipelines/{new_name}")
    except FelderaAPIError as e:
        assert e.error_code == "UnknownPipelineName"
    TEST_CLIENT.http.patch(f"/pipelines/{pipeline_name}", {"name": new_name})
    pipeline_name = new_name
    pipeline = Pipeline.get(pipeline_name, TEST_CLIENT)
    validate_tracking(pipeline_name, current, tracking)
    assert pipeline.name == pipeline_name

    # Edit description (II)
    new_description = "description-2"
    TEST_CLIENT.http.patch(
        f"/pipelines/{pipeline_name}", {"description": new_description}
    )
    assert pipeline.description() == new_description
    validate_tracking(pipeline_name, current, tracking)

    # Edit runtime configuration
    new_runtime_config = {"workers": 3}
    TEST_CLIENT.http.patch(
        f"/pipelines/{pipeline_name}", {"runtime_config": new_runtime_config}
    )
    assert pipeline.runtime_config().workers == 3
    validate_tracking(pipeline_name, current, tracking)

    # Edit program code (SQL)
    new_sql = "CREATE TABLE t1 (i1 INT);"
    TEST_CLIENT.http.patch(f"/pipelines/{pipeline_name}", {"program_code": new_sql})
    assert pipeline.program_code() == new_sql
    pipeline.wait_for_compilation()
    validate_tracking(pipeline_name, current, tracking)

    # Edit UDF Rust
    new_udf_rust = "// Changed"
    TEST_CLIENT.http.patch(f"/pipelines/{pipeline_name}", {"udf_rust": new_udf_rust})
    assert pipeline.udf_rust() == new_udf_rust
    pipeline.wait_for_compilation()
    validate_tracking(pipeline_name, current, tracking)

    # Edit UDF TOML
    new_udf_toml = "# Changed"
    TEST_CLIENT.http.patch(f"/pipelines/{pipeline_name}", {"udf_toml": new_udf_toml})
    assert pipeline.udf_toml() == new_udf_toml
    pipeline.wait_for_compilation()
    validate_tracking(pipeline_name, current, tracking)

    # Edit program configuration
    new_program_config = {"cache": False}
    TEST_CLIENT.http.patch(
        f"/pipelines/{pipeline_name}", {"program_config": new_program_config}
    )
    assert pipeline.program_config()["cache"] == False
    pipeline.wait_for_compilation()
    validate_tracking(pipeline_name, current, tracking)

    # Delete the pipeline
    pipeline.stop(force=True)
    pipeline.delete(clear_storage=True)

    # The tracking will eventually fail because the stream ended abruptly
    with pytest.raises(ChunkedEncodingError):
        for _ in tracking:
            pass

    # Pipeline to track does not exist
    with pytest.raises(FelderaAPIError) as e:
        for _ in pipeline.track():
            pass
    assert e.value.error_code == "UnknownPipelineName"
