import time
from feldera import PipelineBuilder
from feldera.rest.errors import FelderaAPIError
from feldera.enums import PipelineStatus
from .helper import (
    gen_pipeline_name,
    post_json,
    api_url,
)
from tests import TEST_CLIENT


@gen_pipeline_name
def test_pipeline_error_dismissal(pipeline_name):
    sql = """
    CREATE TABLE t1(c1 INTEGER);
    CREATE VIEW v1 AS SELECT ELEMENT(ARRAY [2, 3]) FROM t1;
    """
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, sql).create_or_replace()
    pipeline.stop(force=True)
    pipeline.clear_storage()
    pipeline.dismiss_error()

    # Beginning: no deployment error is present
    assert pipeline.deployment_error() is None

    # Normal start and stop (idempotent): no deployment error is present afterward
    pipeline.start()
    pipeline.start(dismiss_error=True)
    pipeline.start(dismiss_error=False)
    pipeline.dismiss_error()
    pipeline.start()
    pipeline.stop(force=True)
    assert pipeline.deployment_error() is None

    # Without any error present, dismissal works both using the standalone
    # function and as argument for start()
    pipeline.dismiss_error()
    pipeline.start(dismiss_error=True)
    pipeline.stop(force=True)
    assert pipeline.deployment_error() is None
    pipeline.start(dismiss_error=False)
    pipeline.stop(force=True)
    assert pipeline.deployment_error() is None

    # Cannot start pipeline without dismissing error
    pipeline.start()
    cause_error_and_wait_for_stopped(pipeline)
    assert pipeline.deployment_error() is not None
    error_occurred = False
    try:
        pipeline.start(dismiss_error=False)
    except FelderaAPIError as e:
        error_occurred = True
        assert e.status_code == 400
        assert e.error_code == "CannotStartWithUndismissedError"
    assert error_occurred
    assert pipeline.deployment_error() is not None
    pipeline.stop(force=True)
    pipeline.dismiss_error()

    # Can start pipeline with dismissed error (separate call)
    pipeline.start()
    cause_error_and_wait_for_stopped(pipeline)
    assert pipeline.deployment_error() is not None
    pipeline.dismiss_error()
    pipeline.start(dismiss_error=False)
    assert pipeline.deployment_error() is None
    pipeline.stop(force=True)

    # Can start pipeline with dismissed error (integrated)
    pipeline.start()
    cause_error_and_wait_for_stopped(pipeline)
    assert pipeline.deployment_error() is not None
    pipeline.start(dismiss_error=True)
    assert pipeline.deployment_error() is None
    pipeline.stop(force=True)


def cause_error_and_wait_for_stopped(pipeline):
    # This data causes the pipeline to panic
    response = post_json(
        api_url(
            f"/pipelines/{pipeline.name}/ingress/t1?format=json&array=true&update_format=raw"
        ),
        [{"c1": 1}, {"c1": 2}, {"c1": 3}],
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Response should be 200 OK but is: {response.status_code} with body: {response.json()}"
        )

    # It should stop with an error within 60 seconds
    timeout_s = 60
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if pipeline.status() == PipelineStatus.STOPPED:
            return
        time.sleep(0.2)
    raise TimeoutError(
        f"Timed out waiting for pipeline '{pipeline.name}' to stop with an error"
    )
