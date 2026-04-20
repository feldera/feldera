from feldera.enums import PipelineStatus, ProgramStatus, StorageStatus
from feldera.rest.errors import FelderaAPIError
import time
import pytest
from http import HTTPStatus
from feldera import PipelineBuilder, Pipeline
from feldera.enums import BootstrapPolicy
from tests import TEST_CLIENT
from .helper import (
    wait_for_condition,
    create_pipeline,
    post_json,
    http_request,
    wait_for_program_success,
    gen_pipeline_name,
    get_pipeline,
    start_pipeline,
    start_pipeline_as_paused,
    pause_pipeline,
    resume_pipeline,
    stop_pipeline,
    clear_pipeline,
    delete_pipeline,
    cleanup_pipeline,
    api_url,
    adhoc_query_json,
    post_no_body,
)
from tests import enterprise_only
from feldera.testutils import single_host_only


def _wait_for_stopped_with_error(name: str, timeout_s: float = 90.0):
    pipeline = Pipeline.get(name, TEST_CLIENT)
    wait_for_condition(
        "become stopped",
        lambda: pipeline.status() == PipelineStatus.STOPPED,
        timeout_s=timeout_s,
        poll_interval_s=0.5,
    )
    error = pipeline.deployment_error()
    if error is None:
        raise AssertionError("pipeline did stop but not with an error as expected")
    return error


def _ingress(name: str, table: str, body: str):
    r = http_request(
        "POST",
        api_url(f"/pipelines/{name}/ingress/{table}"),
        headers={"Content-Type": "text/plain"},
        data=body,
    )
    return r


@gen_pipeline_name
def test_deploy_pipeline(pipeline_name):
    """
    - Create pipeline with materialized table and view.
    - Start, ingest data, pause, query, restart, query again, stop & clear.
    """
    sql = (
        "CREATE TABLE t1(c1 INTEGER) WITH ('materialized' = 'true'); "
        "CREATE VIEW v1 AS SELECT * FROM t1;"
    )
    create_pipeline(pipeline_name, sql)

    start_pipeline(pipeline_name)
    assert _ingress(pipeline_name, "t1", "1\n2\n3\n").status_code == HTTPStatus.OK
    assert _ingress(pipeline_name, "t1", "4\r\n5\r\n6").status_code == HTTPStatus.OK

    pause_pipeline(pipeline_name)
    got = adhoc_query_json(pipeline_name, "select * from t1 order by c1")
    assert got == [{"c1": i} for i in range(1, 7)]

    resume_pipeline(pipeline_name)
    got = adhoc_query_json(pipeline_name, "select * from t1 order by c1")
    assert got == [{"c1": i} for i in range(1, 7)]

    stop_pipeline(pipeline_name, force=True)
    clear_pipeline(pipeline_name)


@gen_pipeline_name
def test_pipeline_panic(pipeline_name):
    """
    Pipeline that panics at runtime. Verify reported error_code == RuntimeError.WorkerPanic.
    """
    sql = (
        "CREATE TABLE t1(c1 INTEGER); "
        "CREATE VIEW v1 AS SELECT ELEMENT(ARRAY [2, 3]) FROM t1;"
    )
    create_pipeline(pipeline_name, sql)

    start_pipeline(pipeline_name)
    _ingress(pipeline_name, "t1", "1\n2\n3\n")

    err = _wait_for_stopped_with_error(pipeline_name)
    assert err.get("error_code") == "RuntimeError.WorkerPanic"

    stop_pipeline(pipeline_name, force=True)
    clear_pipeline(pipeline_name)


@gen_pipeline_name
def test_pipeline_restart(pipeline_name):
    """
    Start -> stop (force) -> start -> stop (force & clear).
    """
    sql = "CREATE TABLE t1(c1 INTEGER); CREATE VIEW v1 AS SELECT * FROM t1;"
    create_pipeline(pipeline_name, sql)

    start_pipeline(pipeline_name)
    stop_pipeline(pipeline_name, force=True)
    start_pipeline(pipeline_name)
    stop_pipeline(pipeline_name, force=True)
    clear_pipeline(pipeline_name)


@gen_pipeline_name
def test_pipeline_start_without_compiling(pipeline_name):
    """
    Attempt to start before compilation fully finishes (early start).
    Poll until state moves beyond Pending/CompilingSql then start.
    """
    r = post_json(
        api_url("/pipelines"),
        {
            "name": pipeline_name,
            "program_code": "CREATE TABLE foo (bar INTEGER);",
        },
    )
    assert r.status_code == HTTPStatus.CREATED

    # Wait until program status moves beyond early compilation states.
    # Keep a long timeout because parallel test runs can queue compilation.
    pipeline = Pipeline.get(pipeline_name, TEST_CLIENT)
    wait_for_condition(
        "program status moves past Pending/CompilingSql",
        lambda: pipeline.program_status()
        not in (ProgramStatus.Pending, ProgramStatus.CompilingSql),
        timeout_s=1800.0,
        poll_interval_s=1.0,
    )

    start_pipeline(pipeline_name, wait=False)


@gen_pipeline_name
def test_pipeline_deleted_during_program_compilation(pipeline_name):
    """
    Delete pipeline at various intervals during compilation; ensure no server failure
    and later we can still compile a simple program.
    """
    delays = [0, 0.5, 1.0, 1.5, 2.0]
    for idx, delay in enumerate(delays):
        name = f"{pipeline_name}_{idx}"
        cleanup_pipeline(name)
        r = post_json(
            api_url("/pipelines"),
            {
                "name": name,
                "program_code": "",
            },
        )
        assert r.status_code == HTTPStatus.CREATED
        time.sleep(delay)
        dr = delete_pipeline(name)
        assert dr.status_code == HTTPStatus.OK, dr.text

    # Final validation: create a new pipeline and compile successfully
    final_name = pipeline_name
    r = post_json(
        api_url("/pipelines"),
        {"name": final_name, "program_code": ""},
    )
    assert r.status_code == HTTPStatus.CREATED
    wait_for_program_success(final_name, 1)


@gen_pipeline_name
def test_pipeline_stop_force_after_start(pipeline_name):
    """
    Start and then force stop after varying short delays.
    """
    create_pipeline(pipeline_name, "CREATE TABLE t1(c1 INTEGER);")

    for delay_sec in [0, 0.1, 0.5, 1, 3, 10, 20]:
        print(f"Testing with {delay_sec} second delay")
        # Issue non-blocking start
        start_pipeline(pipeline_name, wait=False)
        # Shortly wait for the pipeline to transition to next state(s)
        time.sleep(delay_sec)
        # Stop force and clear the pipeline
        stop_pipeline(pipeline_name, force=True)
        clear_pipeline(pipeline_name)


@gen_pipeline_name
def test_pipeline_stop_with_force(pipeline_name):
    """
    Sequences of starting/stopping with force.
    """
    create_pipeline(pipeline_name, "")

    # Already stopped force
    stop_pipeline(pipeline_name, force=True)

    # Start then immediate stop (force)
    #
    # We do not wait for the pipeline to start, but we do wait for it
    # to transition away from "Stopped".  Otherwise, there is a race:
    #
    # - Request start.
    #
    # - Request force stop.
    #
    # - Check for "stopped" status succeeds because starting up is
    #   taking a little while, so we move along to
    #   start_pipeline_as_paused().
    #
    # - Pipeline transitions to "stopping".
    #
    # - start_pipeline_as_paused() fails with "Cannot restart the
    #   pipeline while it is stopping. Wait until it is stopped before
    #   starting the pipeline again."
    start_pipeline(pipeline_name, wait=False)
    pipeline = Pipeline.get(pipeline_name, TEST_CLIENT)
    wait_for_condition(
        f"{pipeline_name} no longer stopped",
        lambda: pipeline.status() != PipelineStatus.STOPPED,
        timeout_s=30.0,
        poll_interval_s=0.2,
    )
    stop_pipeline(pipeline_name, force=True)

    # Start paused then stop (simulate by pausing immediately)
    start_pipeline_as_paused(pipeline_name)
    stop_pipeline(pipeline_name, force=True)

    # Start, stop (without waiting), then stop again
    start_pipeline(pipeline_name)
    stop_pipeline(pipeline_name, force=True, wait=False)
    stop_pipeline(pipeline_name, force=True)


@enterprise_only
@gen_pipeline_name
def test_pipeline_stop_without_force(pipeline_name):
    """
    Same sequences but without force (Enterprise only).
    """
    create_pipeline(pipeline_name, "")

    # Already stopped
    stop_pipeline(pipeline_name, force=False)

    # Start then stop (non-force)
    #
    # See test_pipeline_stop_with_force() for notes.
    start_pipeline(pipeline_name, wait=False)
    stop_pipeline(pipeline_name, force=False)

    # Start, wait for running, stop
    start_pipeline(pipeline_name)
    stop_pipeline(pipeline_name, force=False)

    # Start paused (pause right away), stop
    start_pipeline_as_paused(pipeline_name)
    stop_pipeline(pipeline_name, force=False)

    # Start, stop twice in a row
    start_pipeline(pipeline_name)
    stop_pipeline(pipeline_name, force=False, wait=False)
    stop_pipeline(pipeline_name, force=False)


@gen_pipeline_name
def test_pipeline_clear(pipeline_name):
    """
    Validate storage_status transitions and clear behavior.
    """
    create_pipeline(pipeline_name, "")

    obj = get_pipeline(pipeline_name, "status").json()
    assert StorageStatus.from_str(obj.get("storage_status")) == StorageStatus.CLEARED

    # Calling /clear does not have an effect
    cr = clear_pipeline(pipeline_name)
    assert cr.status_code == HTTPStatus.ACCEPTED

    # Start (becomes InUse)
    start_pipeline(pipeline_name)
    obj = get_pipeline(pipeline_name, "status").json()
    assert StorageStatus.from_str(obj.get("storage_status")) == StorageStatus.INUSE

    # While running, clear is not possible
    cr = clear_pipeline(pipeline_name)
    assert cr.status_code == HTTPStatus.BAD_REQUEST, cr.text

    # Force stop -> still InUse
    stop_pipeline(pipeline_name, force=True)
    obj = get_pipeline(pipeline_name, "status").json()
    assert StorageStatus.from_str(obj.get("storage_status")) == StorageStatus.INUSE

    # Start then pause
    start_pipeline(pipeline_name)
    pause_pipeline(pipeline_name)
    obj = get_pipeline(pipeline_name, "status").json()
    assert StorageStatus.from_str(obj.get("storage_status")) == StorageStatus.INUSE

    # Clear while paused -> BAD_REQUEST
    cr = clear_pipeline(pipeline_name)
    assert cr.status_code == HTTPStatus.BAD_REQUEST

    # Force stop again, it should still be InUse
    stop_pipeline(pipeline_name, force=True)
    obj = get_pipeline(pipeline_name, "status").json()
    assert StorageStatus.from_str(obj.get("storage_status")) == StorageStatus.INUSE

    # Clear (may go through Clearing then Cleared). Allow two attempts.
    first = clear_pipeline(pipeline_name, wait=False)
    assert first.status_code == HTTPStatus.ACCEPTED
    second = clear_pipeline(pipeline_name, wait=True)
    assert second.status_code == HTTPStatus.ACCEPTED
    assert (
        StorageStatus.from_str(
            get_pipeline(pipeline_name, "status").json().get("storage_status")
        )
        == StorageStatus.CLEARED
    )


@gen_pipeline_name
def test_pipeline_clear_using_api(pipeline_name):
    """
    Validate storage_status transitions and clear behavior using the Python API.
    """
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, "").create_or_replace()

    # Initially should be cleared
    assert pipeline.storage_status() == StorageStatus.CLEARED

    # Clearing should not fail or have an effect while cleared
    pipeline.clear_storage()
    assert pipeline.storage_status() == StorageStatus.CLEARED

    # Starting should make it in-use
    pipeline.start()
    assert pipeline.storage_status() == StorageStatus.INUSE

    # While running, clear is not possible, and it should still be in-use
    error_code = None
    try:
        pipeline.clear_storage()
    except FelderaAPIError as e:
        error_code = e.error_code
    assert error_code == "StorageStatusImmutableUnlessStopped"
    assert pipeline.storage_status() == StorageStatus.INUSE

    # The same for non-blocking clear
    error_code = None
    try:
        pipeline.clear_storage(wait=False)
    except FelderaAPIError as e:
        error_code = e.error_code
    assert error_code == "StorageStatusImmutableUnlessStopped"
    assert pipeline.storage_status() == StorageStatus.INUSE

    # After stopping, it should still be in-use
    pipeline.stop(force=True)
    assert pipeline.storage_status() == StorageStatus.INUSE

    # Starting again makes it remain in use
    pipeline.start()
    assert pipeline.storage_status() == StorageStatus.INUSE
    pipeline.stop(force=True)

    # Clearing it should work when stopped
    assert pipeline.storage_status() == StorageStatus.INUSE
    pipeline.clear_storage()
    assert pipeline.storage_status() == StorageStatus.CLEARED

    # Non-blocking clear should work as well
    pipeline.start()
    pipeline.stop(force=True)
    pipeline.clear_storage(wait=False)
    assert pipeline.storage_status() in [StorageStatus.CLEARING, StorageStatus.CLEARED]

    # Start just after might yield an error if it is still clearing
    try:
        pipeline.start()
    except FelderaAPIError as e:
        assert e.error_code == "CannotStartWhileClearingStorage"
    pipeline.stop(force=True)
    pipeline.clear_storage()


@gen_pipeline_name
def test_pipeline_clear_while_desired_provisioned(pipeline_name):
    """
    This tests the following scenario:
    - There is a pipeline that is stopped (`resources_status=Stopped`) and has
      state in storage (`storage_status=InUse`).
    - The pipeline is started (`resources_desired_status=Provisioned`) without
      waiting. It does not transition yet its `resources_status`. In order to make
      sure this fact is not based solely on quick timing, the test first started
      recompiling the program which takes a few seconds.
    - Before it transitions to `Provisioning` the user attempts to clear the pipeline.
      This should fail.
    """
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, "").create_or_replace()
    pipeline.start()
    pipeline.stop(force=True)
    TEST_CLIENT.patch_pipeline(name=pipeline_name, sql="CREATE TABLE t1 (c1 INT);")
    pipeline.start(wait=False)
    error_code = None
    try:
        pipeline.clear_storage(wait=False)
    except FelderaAPIError as e:
        error_code = e.error_code
    assert error_code == "StorageStatusImmutableUnlessStopped", (
        f"User was able to clear storage without error or got the wrong error (error={error_code}), which shouldn't happen"
    )


@gen_pipeline_name
def test_start_as_standby_fails(pipeline_name):
    """
    Unable to start as standby if runtime configuration requirements are not met.
    """
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": ""})
    assert r.status_code == HTTPStatus.CREATED
    r = post_no_body(
        api_url(f"/pipelines/{pipeline_name}/start"), params={"initial": "standby"}
    )
    assert r.status_code == HTTPStatus.BAD_REQUEST
    assert r.json()["error_code"] == "InitialStandbyNotAllowed"


@gen_pipeline_name
def test_pipeline_bootstrap_policy_is_removed(pipeline_name):
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, "").create_or_replace()
    for expectation in [
        BootstrapPolicy.ALLOW,
        BootstrapPolicy.REJECT,
        BootstrapPolicy.AWAIT_APPROVAL,
    ]:
        assert pipeline.bootstrap_policy() is None
        pipeline.start(bootstrap_policy=expectation)
        assert pipeline.bootstrap_policy() == expectation
        assert pipeline.bootstrap_policy() is not None
        pipeline.stop(force=True)
        assert pipeline.bootstrap_policy() is None


@gen_pipeline_name
def test_pipeline_double_start(pipeline_name):
    """
    Tests what calling the pipeline start multiple times works, as long as the
    bootstrap policy and initial are not changed.

    TODO: testing `initial=standby` requires setting up remote storage, and as
          such is commented out for now. It already tests the underlying mechanism
          using `initial=running` and `initial=paused`, and `initial=standby`
          is not a special case.
    """
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, "").create_or_replace()

    # OK: basic
    pipeline.start()
    pipeline.start()
    pipeline.stop(force=True)

    # OK: same bootstrap policy
    for b in [
        BootstrapPolicy.ALLOW,
        BootstrapPolicy.REJECT,
        BootstrapPolicy.AWAIT_APPROVAL,
    ]:
        pipeline.start(bootstrap_policy=b)
        pipeline.start(bootstrap_policy=b)
        pipeline.stop(force=True)

    # OK: same initial
    pipeline.start()
    pipeline.start()
    pipeline.stop(force=True)
    pipeline.start_paused()
    pipeline.start_paused()
    pipeline.stop(force=True)
    # pipeline.start_standby()
    # pipeline.start_standby()
    # pipeline.stop(force=True)

    # FAIL: different bootstrap policy
    for b1, b2 in [
        (BootstrapPolicy.ALLOW, BootstrapPolicy.REJECT),
        (BootstrapPolicy.ALLOW, BootstrapPolicy.AWAIT_APPROVAL),
        (BootstrapPolicy.REJECT, BootstrapPolicy.ALLOW),
        (BootstrapPolicy.REJECT, BootstrapPolicy.AWAIT_APPROVAL),
        (BootstrapPolicy.AWAIT_APPROVAL, BootstrapPolicy.ALLOW),
        (BootstrapPolicy.AWAIT_APPROVAL, BootstrapPolicy.REJECT),
    ]:
        pipeline.start(bootstrap_policy=b1)
        with pytest.raises(FelderaAPIError) as e:
            pipeline.start(bootstrap_policy=b2)
        assert e.value.error_code == "BootstrapPolicyImmutableUnlessStopped"
        pipeline.stop(force=True)

    # FAIL: different initial
    pipeline.start()
    with pytest.raises(FelderaAPIError) as e:
        pipeline.start_paused()
    assert e.value.error_code == "InitialImmutableUnlessStopped"
    pipeline.stop(force=True)
    pipeline.start_paused()
    with pytest.raises(FelderaAPIError) as e:
        pipeline.start()
    assert e.value.error_code == "InitialImmutableUnlessStopped"
    pipeline.stop(force=True)
    # pipeline.start_paused()
    # pipeline.start_standby()
    # pipeline.stop(force=True)


@gen_pipeline_name
@single_host_only
def test_pipeline_storage_status_details_without_checkpoints(pipeline_name):
    """
    Validate storage_status_details transitions and clear behavior using the Python API
    without checkpoints.
    """
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, "").create_or_replace()

    # Initially no details
    assert pipeline.storage_status() == StorageStatus.CLEARED
    assert pipeline.storage_status_details() is None

    # Clearing again will still yield no details
    pipeline.clear_storage()
    assert pipeline.storage_status() == StorageStatus.CLEARED
    assert pipeline.storage_status_details() is None

    # Starting should make it in-use with no checkpoints
    pipeline.start()
    assert pipeline.storage_status() == StorageStatus.INUSE
    assert pipeline.storage_status_details() == {"checkpoints": []}

    # After stopping, the details should still be available
    pipeline.stop(force=True)
    assert pipeline.storage_status() == StorageStatus.INUSE
    assert pipeline.storage_status_details() == {"checkpoints": []}

    # Starting and stopping should not affect the details
    pipeline.start()
    assert pipeline.storage_status() == StorageStatus.INUSE
    assert pipeline.storage_status_details() == {"checkpoints": []}
    pipeline.stop(force=True)
    assert pipeline.storage_status() == StorageStatus.INUSE
    assert pipeline.storage_status_details() == {"checkpoints": []}

    # Clearing it should clear away any details
    pipeline.clear_storage()
    assert pipeline.storage_status() == StorageStatus.CLEARED
    assert pipeline.storage_status_details() is None


@gen_pipeline_name
@single_host_only
@enterprise_only
def test_pipeline_storage_status_details_with_checkpoints(pipeline_name):
    """
    Validate storage_status_details transitions and clear behavior using the Python API
    with checkpoints.
    """
    pipeline = PipelineBuilder(
        TEST_CLIENT,
        pipeline_name,
        """
    CREATE TABLE t1 (
        val INT
    ) WITH (
        'materialized' = 'true',
        'connectors' = '[{
            "transport": {
                "name": "datagen",
                "config": {
                    "plan": [{
                        "limit": 1000000,
                        "rate": 1,
                        "fields": {
                            "val": { "strategy": "uniform", "range": [0, 1000000] }
                        }
                    }]
                }
            }
        }]'
    );
    """,
    ).create_or_replace()

    # Initially no details
    assert pipeline.storage_status() == StorageStatus.CLEARED
    assert pipeline.storage_status_details() is None

    # Clearing again will still yield no details
    pipeline.clear_storage()
    assert pipeline.storage_status() == StorageStatus.CLEARED
    assert pipeline.storage_status_details() is None

    # Starting should make it in-use with no checkpoints
    pipeline.start()
    assert pipeline.storage_status() == StorageStatus.INUSE
    assert pipeline.storage_status_details() == {"checkpoints": []}

    # Explicitly perform a checkpoint
    pipeline.checkpoint(wait=True)

    # Check one checkpoint was created
    assert pipeline.storage_status() == StorageStatus.INUSE
    time.sleep(20)
    details = pipeline.storage_status_details()
    assert len(details["checkpoints"]) == 1

    # Explicitly perform another checkpoint
    pipeline.checkpoint(wait=True)

    # Check another checkpoint was made
    assert pipeline.storage_status() == StorageStatus.INUSE
    time.sleep(20)
    details = pipeline.storage_status_details()
    assert len(details["checkpoints"]) == 2

    # After stopping, the details should still be available
    pipeline.stop(force=True)
    assert pipeline.storage_status() == StorageStatus.INUSE
    assert pipeline.storage_status_details() == details

    # Starting and stopping should not affect the details
    pipeline.start()
    assert pipeline.storage_status() == StorageStatus.INUSE
    assert pipeline.storage_status_details() == details
    pipeline.stop(force=True)
    assert pipeline.storage_status() == StorageStatus.INUSE
    assert pipeline.storage_status_details() == details

    # Clearing it should clear away any details
    pipeline.clear_storage()
    assert pipeline.storage_status() == StorageStatus.CLEARED
    assert pipeline.storage_status_details() is None


@gen_pipeline_name
def test_refresh_version(pipeline_name):
    """
    The `refresh_version` should only be sparingly incremented over the lifetime of a pipeline.
    The refresh versions in this test are approximate as resources, runtime and storage status
    details can be updated over time during deployment.
    """
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, "").create_or_replace()
    assert (
        TEST_CLIENT.http.get(f"/pipelines/{pipeline_name}?selector=status")[
            "refresh_version"
        ]
        == 3
    )
    pipeline.start()
    time.sleep(30.0)
    assert (
        TEST_CLIENT.http.get(f"/pipelines/{pipeline_name}?selector=status")[
            "refresh_version"
        ]
        <= 10
    )
    pipeline.stop(force=True)
    pipeline.clear_storage()
    assert (
        TEST_CLIENT.http.get(f"/pipelines/{pipeline_name}?selector=status")[
            "refresh_version"
        ]
        <= 15
    )
