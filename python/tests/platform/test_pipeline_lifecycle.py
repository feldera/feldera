import time
from http import HTTPStatus

from .helper import (
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
    wait_for_deployment_status,
    api_url,
    adhoc_query_json,
    post_no_body,
)
from tests import enterprise_only


def _wait_for_stopped_with_error(name: str, timeout_s: float = 90.0):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = get_pipeline(name, "status")
        if r.status_code == HTTPStatus.OK:
            obj = r.json()
            if obj.get("deployment_status") == "Stopped":
                err = obj.get("deployment_error")
                if err:
                    return err
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for pipeline '{name}' to stop with an error")


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
    r = post_json(
        api_url("/pipelines"),
        {"name": pipeline_name, "program_code": sql},
    )
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)

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
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)

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
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": sql})
    assert r.status_code == HTTPStatus.CREATED, r.text
    wait_for_program_success(pipeline_name, 1)

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

    # Poll status transitions

    # TODO reduce with parallel compilation
    # 30minutes because we compile a lot of tests in parallel so things might be queued for along time
    max_deadline = 1800
    deadline = time.time() + max_deadline
    while time.time() < deadline:
        obj = get_pipeline(pipeline_name, "status").json()
        status = obj.get("program_status")
        if status not in ("Pending", "CompilingSql"):
            break
        time.sleep(1)
    else:
        raise TimeoutError(
            f"Took longer than {max_deadline} seconds to move past CompilingSql"
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
    r = post_json(
        api_url("/pipelines"),
        {"name": pipeline_name, "program_code": "CREATE TABLE t1(c1 INTEGER);"},
    )
    assert r.status_code == HTTPStatus.CREATED
    wait_for_program_success(pipeline_name, 1)

    for delay_sec in [0, 0.1, 0.5, 1, 3, 10]:
        start_pipeline(pipeline_name)
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
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": ""})
    assert r.status_code == HTTPStatus.CREATED
    wait_for_program_success(pipeline_name, 1)

    # Already stopped force
    stop_pipeline(pipeline_name, force=True)

    # Start then immediate stop (force)
    start_pipeline(pipeline_name, False)
    stop_pipeline(pipeline_name, force=True)

    # Start paused then stop (simulate by pausing immediately)
    start_pipeline_as_paused(pipeline_name)
    wait_for_deployment_status(pipeline_name, "Paused", 30)
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
    r = post_json(api_url("/pipelines"), {"name": pipeline_name, "program_code": ""})
    assert r.status_code == HTTPStatus.CREATED
    wait_for_program_success(pipeline_name, 1)

    # Already stopped
    stop_pipeline(pipeline_name, force=False)

    # Start then stop (non-force)
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
    r = post_json(
        api_url("/pipelines"),
        {"name": pipeline_name, "program_code": ""},
    )
    assert r.status_code == HTTPStatus.CREATED
    wait_for_program_success(pipeline_name, 1)

    obj = get_pipeline(pipeline_name, "status").json()
    assert obj.get("storage_status") == "Cleared"

    # Calling /clear does not have an effect
    cr = clear_pipeline(pipeline_name)
    assert cr.status_code == HTTPStatus.ACCEPTED

    # Start (becomes InUse)
    start_pipeline(pipeline_name)
    obj = get_pipeline(pipeline_name, "status").json()
    assert obj.get("storage_status") == "InUse"

    # While running, clear is not possible
    cr = clear_pipeline(pipeline_name)
    assert cr.status_code == HTTPStatus.BAD_REQUEST, cr.text

    # Force stop -> still InUse
    stop_pipeline(pipeline_name, force=True)
    obj = get_pipeline(pipeline_name, "status").json()
    assert obj.get("storage_status") == "InUse"

    # Start then pause
    start_pipeline(pipeline_name)
    pause_pipeline(pipeline_name)
    obj = get_pipeline(pipeline_name, "status").json()
    assert obj.get("storage_status") == "InUse"

    # Clear while paused -> BAD_REQUEST
    cr = clear_pipeline(pipeline_name)
    assert cr.status_code == HTTPStatus.BAD_REQUEST

    # Force stop again, it should still be InUse
    stop_pipeline(pipeline_name, force=True)
    obj = get_pipeline(pipeline_name, "status").json()
    assert obj.get("storage_status") == "InUse"

    # Clear (may go through Clearing then Cleared). Allow two attempts.
    first = clear_pipeline(pipeline_name, wait=False)
    assert first.status_code == HTTPStatus.ACCEPTED
    second = clear_pipeline(pipeline_name, wait=True)
    assert second.status_code == HTTPStatus.ACCEPTED
    assert (
        get_pipeline(pipeline_name, "status").json().get("storage_status") == "Cleared"
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
