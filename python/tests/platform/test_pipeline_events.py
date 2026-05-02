from datetime import datetime
from feldera import PipelineBuilder
from tests import TEST_CLIENT
from .helper import gen_pipeline_name
import time


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


def check_fields_event_all(event: dict):
    assert set(event.keys()) == {
        "event_id",
        "recorded_at",
        "deployment_resources_status",
        "deployment_resources_status_details",
        "deployment_resources_desired_status",
        "deployment_runtime_status",
        "deployment_runtime_status_details",
        "deployment_runtime_desired_status",
        "deployment_has_error",
        "deployment_error",
        "program_status",
        "storage_status",
        "storage_status_details",
    }
    if event["deployment_runtime_status"] is None:
        assert event["deployment_runtime_status_details"] is None
    else:
        assert event["deployment_runtime_status_details"] is not None
    if event["deployment_has_error"]:
        assert event["deployment_error"] is not None


def check_fields_event_status(event: dict):
    assert set(event.keys()) == {
        "event_id",
        "recorded_at",
        "deployment_resources_status",
        "deployment_resources_desired_status",
        "deployment_runtime_status",
        "deployment_runtime_desired_status",
        "deployment_has_error",
        "program_status",
        "storage_status",
    }


@gen_pipeline_name
def test_events(pipeline_name):
    # Perform several operations on the pipeline
    sql = "CREATE TABLE t1(i1 INTEGER); CREATE VIEW v1 AS SELECT * FROM t1;"
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, sql).create_or_replace()
    pipeline.start()
    pipeline.pause()
    pipeline.resume()
    pipeline.stop(force=True)
    pipeline.clear_storage()

    # Create another pipeline
    pipeline_name_other = f"{pipeline_name}-other"
    pipeline_other = PipelineBuilder(
        TEST_CLIENT, pipeline_name_other, sql
    ).create_or_replace()
    pipeline_other.clear_storage()
    pipeline_other.start()
    pipeline_other.stop(force=True)
    pipeline_other.clear_storage()

    # Python API instance of the pipeline
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

    ###############################
    ## Event fields are consistent

    # All events
    events_status = pipeline.events(selector="status")
    assert events_status == pipeline.events()  # Default selector is status
    events_all = pipeline.events(selector="all")

    # Latest event
    latest_event_status = pipeline.event("latest", selector="status")
    assert latest_event_status == pipeline.event("latest")  # Default selector is status
    latest_event_all = pipeline.event("latest", selector="all")

    # Latest event should be the first in the list
    assert events_status[0] == latest_event_status
    assert events_all[0] == latest_event_all

    # Check all events have the expected fields
    for event in events_status:
        check_fields_event_status(event)
    for event in events_all:
        check_fields_event_all(event)

    # Check events are ordered descendingly based on record timestamp
    prev_event = None
    for i, event in enumerate(events_status):
        assert prev_event is None or datetime.fromisoformat(
            event["recorded_at"].replace("Z", "+00:00")
        ) <= datetime.fromisoformat(prev_event["recorded_at"].replace("Z", "+00:00"))
        assert event["recorded_at"] == events_all[i]["recorded_at"]
        prev_event = event

    # Check that individually retrieving the events has the same outcome
    events_status_one_by_one = []
    for event in events_status:
        events_status_one_by_one.append(
            pipeline.event(event["event_id"], selector="status")
        )
    assert events_status == events_status_one_by_one
    events_all_one_by_one = []
    for event in events_status:
        events_all_one_by_one.append(pipeline.event(event["event_id"], selector="all"))
    assert events_all == events_all_one_by_one

    ########################################
    ## Check statuses occurred as expected

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
    events_status_limited = list(
        map(
            lambda e1: (
                e1["deployment_resources_status"],
                e1["deployment_resources_desired_status"],
                e1["deployment_runtime_status"],
                e1["deployment_runtime_desired_status"],
                e1["deployment_has_error"],
                e1["program_status"],
                e1["storage_status"],
            ),
            events_status,
        )
    )

    # fmt: off
    assert remove_consecutive_duplicates(events_status_limited) == [
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
        ("Stopped", "Stopped", None, None, False, "Pending", "Cleared"),
    ]
    # fmt: on


@gen_pipeline_name
def test_events_retained(pipeline_name):
    # Test that the events retention is enforced.

    # Perform actions that result in at least 1000 events
    sql = "CREATE TABLE t1(i1 INTEGER); CREATE VIEW v1 AS SELECT * FROM t1;"
    pipeline = PipelineBuilder(TEST_CLIENT, pipeline_name, sql).create_or_replace()
    pipeline.start()
    for i in range(500):
        pipeline.pause()
        pipeline.resume()
    pipeline.stop(force=True)

    # Check every second for 80 seconds (30 seconds is the cleanup interval)
    # if the number of events has become exactly 720 (the default retention number).
    num_events = -1
    for _ in range(80):
        num_events = len(pipeline.events())
        if num_events == 720:
            # Early break to speed up the test if it has already done the cleanup
            break
        time.sleep(1)
    assert num_events == 720
