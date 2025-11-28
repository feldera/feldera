from http import HTTPStatus
import time

from .helper import get, API_PREFIX


def test_cluster_events():
    """
    Checks the cluster events endpoints for list, latest and specific event.
    """
    # Get latest event (extended)
    response = get(f"{API_PREFIX}/cluster/events/latest")
    latest_event_extended = response.json()
    assert response.status_code == HTTPStatus.OK, f"Expected 200 OK but got {response.status_code}"

    # Get latest event
    response = get(f"{API_PREFIX}/cluster/events/latest?selector=status")
    latest_event = response.json()
    assert response.status_code == HTTPStatus.OK, f"Expected 200 OK but got {response.status_code}"

    # Retrieve specific event (extended)
    response = get(f"{API_PREFIX}/cluster/events/{latest_event['id']}")
    specific_event_extended = response.json()
    assert response.status_code == HTTPStatus.OK, f"Expected 200 OK but got {response.status_code}"

    # Retrieve specific event
    response = get(f"{API_PREFIX}/cluster/events/{latest_event['id']}?selector=status")
    specific_event = response.json()
    assert response.status_code == HTTPStatus.OK, f"Expected 200 OK but got {response.status_code}"

    # Retrieve all events
    response = get(f"{API_PREFIX}/cluster/events")
    all_events = response.json()
    assert response.status_code == HTTPStatus.OK, f"Expected 200 OK but got {response.status_code}"

    # Test values
    assert latest_event in all_events, "Latest event is not in full events list"
    assert specific_event == latest_event , "Specifically retrieved event does not match the latest event"
    assert specific_event_extended == latest_event_extended , "Specifically retrieved extended event does not match the latest event"
