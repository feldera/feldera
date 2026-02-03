from tests import TEST_CLIENT

EXPECTED_ALL_FIELDS = [
    "id",
    "recorded_at",
    "all_healthy",
    "api_status",
    "api_self_info",
    "api_resources_info",
    "compiler_status",
    "compiler_self_info",
    "compiler_resources_info",
    "runner_status",
    "runner_self_info",
    "runner_resources_info",
]
EXPECTED_STATUS_FIELDS = [
    "id",
    "recorded_at",
    "all_healthy",
    "api_status",
    "compiler_status",
    "runner_status",
]


def check_event_all(event):
    assert sorted(event.keys()) == sorted(EXPECTED_ALL_FIELDS)


def check_event_status(event):
    assert sorted(event.keys()) == sorted(EXPECTED_STATUS_FIELDS)


def test_cluster_events():
    """
    Checks the cluster events endpoints for latest event, specific event and list of events.
    """
    latest_event_all = TEST_CLIENT.get_cluster_event("latest", selector="all")
    latest_event_status = TEST_CLIENT.get_cluster_event("latest")
    specific_event_all = TEST_CLIENT.get_cluster_event(
        latest_event_all["id"], selector="all"
    )
    specific_event_status = TEST_CLIENT.get_cluster_event(latest_event_status["id"])
    all_events_status = TEST_CLIENT.get_cluster_events()
    assert latest_event_status in all_events_status, (
        "Latest event is not in full events list"
    )
    assert latest_event_status == specific_event_status, (
        "Specifically retrieved event does not match the latest event"
    )
    assert latest_event_all == specific_event_all, (
        "Specifically retrieved extended event does not match the latest event"
    )
    check_event_all(latest_event_all)
    check_event_all(specific_event_all)
    check_event_status(latest_event_status)
    check_event_status(specific_event_status)
    for event in all_events_status:
        check_event_status(event)
