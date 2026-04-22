from datetime import datetime
from http import HTTPStatus
from feldera import Pipeline
from tests import TEST_CLIENT
from .helper import (
    create_pipeline,
    gen_pipeline_name,
    start_pipeline,
    pause_pipeline,
    resume_pipeline,
    stop_pipeline,
    clear_pipeline,
    get_pipeline_events,
    get_pipeline_event,
    cleanup_pipeline,
)


def remove_consecutive_duplicates(v: list[dict]):
    """
    Creates a new list based on the provided list, but with consecutive duplicate items removed.

    :param v: List of items.
    :return: List without duplicate consecutive items.
    """
    if len(v) <= 1:
        if len(v) == 0:
            return []
        else:
            return [v[0]]
    else:
        result = [v[0]]
        prev = v[0]
        for i in range(1, len(v)):
            if v[i] != prev:
                result.append(v[i])
            prev = v[i]
        return result


@gen_pipeline_name
def test_events(pipeline_name):
    # Perform several operations on the pipeline
    sql = "CREATE TABLE t1(i1 INTEGER); CREATE VIEW v1 AS SELECT * FROM t1;"
    create_pipeline(pipeline_name, sql)
    start_pipeline(pipeline_name)
    pause_pipeline(pipeline_name)
    resume_pipeline(pipeline_name)
    stop_pipeline(pipeline_name, force=True)
    clear_pipeline(pipeline_name)

    # Create another pipeline
    pipeline_name_other = f"{pipeline_name}-other"
    cleanup_pipeline(pipeline_name_other)
    create_pipeline(pipeline_name_other, sql)
    start_pipeline(pipeline_name_other)
    stop_pipeline(pipeline_name_other, force=True)
    clear_pipeline(pipeline_name_other)

    # Python API instance of the pipeline
    pipeline = Pipeline.get(pipeline_name, TEST_CLIENT)
    pipeline.modify(sql="invalid-sql")
    compile_error = None
    try:
        TEST_CLIENT._wait_for_compilation(pipeline_name, None)
    except RuntimeError as e:
        compile_error = e
    assert compile_error is not None

    # Attempt to start with a failed compilation
    start_error = None
    try:
        pipeline.start()
    except RuntimeError as e:
        start_error = e
    assert start_error is not None
    assert (
        pipeline.deployment_error()["error_code"] == "StartFailedDueToFailedCompilation"
    )
    pipeline.dismiss_error()

    # Check the events this all should have generated
    response = get_pipeline_events(pipeline_name)
    assert response.status_code == HTTPStatus.OK
    events = response.json()
    assert events == pipeline.events()

    # Check events are ordered descendingly based on record timestamp
    prev_event = None
    for event in events:
        assert prev_event is None or datetime.fromisoformat(
            event["recorded_at"].replace("Z", "+00:00")
        ) <= datetime.fromisoformat(prev_event["recorded_at"].replace("Z", "+00:00"))
        prev_event = event

    # Test deduplication function
    assert remove_consecutive_duplicates([]) == []
    assert remove_consecutive_duplicates([{"a": 1}]) == [{"a": 1}]
    assert remove_consecutive_duplicates([{"a": 1}, {"a": 1}, {"a": 1}]) == [{"a": 1}]
    assert remove_consecutive_duplicates([{"a": 1}, {"a": 1}, {"a": 2}, {"b": 2}]) == [
        {"a": 1},
        {"a": 2},
        {"b": 2},
    ]
    assert remove_consecutive_duplicates([{"a": 1}, {"b": 2}, {"b": 2}]) == [
        {"a": 1},
        {"b": 2},
    ]

    # Map events such that evolution test can be written cleanly
    # Consecutive events are removed because it is possible for events to be repeated if a status takes a longer time
    events_status = list(
        map(
            lambda e: (
                e["resources_status"],
                e["resources_desired_status"],
                e["runtime_status"],
                e["runtime_desired_status"],
                e["has_error"],
                e["program_status"],
                e["storage_status"],
            ),
            events,
        )
    )

    # fmt: off
    assert remove_consecutive_duplicates(events_status) == [
        ("Stopped", "Stopped", None, None, False, "SqlError", "Cleared"),
        ("Stopped", "Stopped", None, None, True, "SqlError", "Cleared"),
        ("Stopping", "Stopped", None, None, True, "SqlError", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "SqlError", "Cleared"),
        ("Stopped", "Provisioned", None, None, False, "SqlError", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "SqlError", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "CompilingSql", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "Pending", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "Success", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "Success", "Clearing"),
        ("Stopped", "Stopped", None, None, False, "Success", "InUse"),
        ("Stopping", "Stopped", None, None, False, "Success", "InUse"),
        ("Provisioned", "Stopped", "Running", "Running", False, "Success", "InUse"),
        ("Provisioned", "Provisioned", "Running", "Running", False, "Success", "InUse"),
        ("Provisioned", "Provisioned", "Paused", "Paused", False, "Success", "InUse"),
        ("Provisioned", "Provisioned", "Running", "Running", False, "Success", "InUse"),
        ("Provisioned", "Provisioned", "Initializing", "Running", False, "Success", "InUse"),
        ("Provisioning", "Provisioned", None, None, False, "Success", "InUse"),
        ("Stopped", "Provisioned", None, None, False, "Success", "InUse"),
        ("Stopped", "Provisioned", None, None, False, "Success", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "Success", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "CompilingRust", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "SqlCompiled", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "CompilingSql", "Cleared"),
        ("Stopped", "Stopped", None, None, False, "Pending", "Cleared")
    ]

    # Check that all events have resources details, and only some have runtime status details
    for event in events:
        response = get_pipeline_event(pipeline_name, event["id"], "all")
        assert response.status_code == HTTPStatus.OK
        detailed_event = response.json()
        assert detailed_event == pipeline.event(event["id"], "all")
        assert "resources_status_details" in detailed_event
        assert detailed_event["resources_status_details"] is not None
        if event["runtime_status"] is None:
            assert detailed_event["runtime_status_details"] is None
        else:
            assert detailed_event["runtime_status_details"] is not None
        assert "storage_status_details" in detailed_event
        assert "error" in detailed_event
        if event["has_error"]:
            assert detailed_event["error"] is not None

    # Check latest event
    response = get_pipeline_event(pipeline_name, "latest", "status")
    assert response.status_code == HTTPStatus.OK
    latest_event = response.json()
    assert latest_event == pipeline.event("latest", "status")
    assert events[0] == latest_event
