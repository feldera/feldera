from http import HTTPStatus
import time

from .helper import get, API_PREFIX


def test_cluster_health_check():
    """
    Poll the cluster health endpoint until both runner and compiler are healthy.

    - Repeatedly query the cluster health endpoint.
    - If both runner and compiler report healthy=true, expect HTTP 200 and stop.
    - Otherwise expect HTTP 503 while waiting.
    - Timeout after 300 seconds.
    """
    # Endpoint used by the Rust test client (mirrored here).
    endpoint = f"{API_PREFIX}/cluster_healthz"

    interval_s = 2
    timeout_s = 300
    start = time.time()

    while True:
        resp = get(endpoint)
        status_code = resp.status_code

        try:
            body = resp.json()
        except Exception:
            raise AssertionError(
                f"Invalid JSON response from cluster health endpoint: status={status_code}, body={resp.text!r}"
            )

        all_healthy = bool(body.get("all_healthy", False) is True)
        if all_healthy:
            # Both components healthy; must be HTTP 200.
            assert status_code == HTTPStatus.OK, (
                f"Expected 200 when both runner and compiler are healthy; got {status_code}, body={body}"
            )
            break
        else:
            # While either component unhealthy, the API should signal service unavailable.
            assert status_code == HTTPStatus.SERVICE_UNAVAILABLE, (
                f"Expected 503 while unhealthy; got {status_code}, body={body}"
            )

        if time.time() - start > timeout_s:
            raise TimeoutError(
                f"Timed out waiting for runner and compiler to become healthy (last body={body})"
            )

        time.sleep(interval_s)


def test_health_check():
    """
    Poll the /healthz endpoint until it reports overall healthy or timeouts.

    Success condition:
      status code 200 and body == {"status": "healthy"}

    Acceptable transient condition (database not ready yet):
      status code 500 and body == {
        "status": "unhealthy: unable to reach database (see logs for further details)"
      }
    """
    endpoint = "/healthz"  # Not versioned in the Rust tests.
    max_attempts = 30
    attempt = 0

    while True:
        resp = get(endpoint)
        status = resp.status_code
        try:
            body = resp.json()
        except Exception:
            raise AssertionError(
                f"Invalid JSON from health endpoint: status={status}, body={resp.text!r}"
            )

        if status == HTTPStatus.OK and body == {"status": "healthy"}:
            # Healthy
            return
        elif status == HTTPStatus.INTERNAL_SERVER_ERROR and body == {
            "status": "unhealthy: unable to reach database (see logs for further details)"
        }:
            # Still unhealthy; keep polling within limit.
            if attempt >= max_attempts:
                raise TimeoutError(
                    f"Took too long for health check to return healthy "
                    f"(last status={status}, body={body})"
                )
        else:
            raise AssertionError(
                f"Unexpected health check response: status={status}, body={body}"
            )

        attempt += 1
        time.sleep(1)
