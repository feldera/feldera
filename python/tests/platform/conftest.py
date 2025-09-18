"""
pytest configuration for platform tests.

Provides shared fixtures for OIDC authentication caching across pytest workers.
Uses pytest-xdist hooks to ensure OIDC token fetching happens only once on the master node.
"""

import pytest
import time
import os
import json
import requests


def is_master(config) -> bool:
    """True if the code running is in the xdist master node or not using xdist at all."""
    return not hasattr(config, "workerinput")


def _fetch_oidc_token():
    """Fetch OIDC token using Resource Owner Password Grant flow."""
    from feldera.testutils_oidc import get_oidc_test_helper

    oidc_helper = get_oidc_test_helper()
    if oidc_helper is None:
        return None

    print("🔐 AUTH: Fetching OIDC token from master node")

    try:
        token_endpoint = oidc_helper.get_token_endpoint()
        data = {
            "grant_type": "password",
            "username": oidc_helper.config.username,
            "password": oidc_helper.config.password,
            "client_id": oidc_helper.config.client_id,
            "client_secret": oidc_helper.config.client_secret,
            "scope": oidc_helper.config.scope,
            "audience": "feldera-api",
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        response = requests.post(token_endpoint, data=data, headers=headers, timeout=30)

        if not response.ok:
            print(f"🔐 AUTH: ❌ Token request FAILED: {response.status_code}")
            raise Exception(
                f"Token request failed: {response.status_code} - {response.text}"
            )

        token_response = response.json()
        print("🔐 AUTH: ✅ Token request SUCCESS!")

        access_token = token_response["access_token"]
        expires_in = token_response.get("expires_in", 3600)
        expires_at = time.time() + expires_in

        return {
            "access_token": access_token,
            "expires_at": expires_at,
            "cached_at": time.time(),
        }

    except Exception as e:
        print(f"🔐 AUTH: CRITICAL FAILURE - Failed to fetch OIDC token: {e}")
        raise RuntimeError(
            f"OIDC authentication is configured but token retrieval failed: {e}"
        ) from e


def pytest_configure(config):
    """Configure hook: fetch OIDC token on master node only."""
    if is_master(config):
        # This runs only on the master node (or in single-node mode)
        token_data = _fetch_oidc_token()
        if token_data:
            print("🔐 AUTH: Master node cached OIDC token for distribution to workers")
            # Store token data in config for distribution to workers
            config.oidc_token_data = token_data

            # CRITICAL: Set environment variables for cross-process token sharing
            import base64

            token_json = json.dumps(token_data)
            token_b64 = base64.b64encode(token_json.encode()).decode()
            os.environ["FELDERA_PYTEST_OIDC_TOKEN"] = token_b64
            print("🔐 AUTH: Token stored in environment for cross-process access")
        else:
            print("🔐 AUTH: No OIDC configuration found, using fallback authentication")
            config.oidc_token_data = None


def pytest_configure_node(node):
    """xdist hook: pass token data to worker nodes via workerinput for fixture access."""
    # Send the token data from master to worker (used by fixture as fallback)
    token_data = getattr(node.config, "oidc_token_data", None)
    node.workerinput["oidc_token_data"] = token_data


@pytest.fixture(scope="session", autouse=True)
def oidc_token_fixture(request):
    """
    Session-scoped fixture that verifies OIDC token setup.

    The actual token fetching is done by pytest_configure hooks and stored
    in environment variables for cross-process access.
    """
    # Token is accessed via environment variable - this fixture just verifies setup
    env_token = os.getenv("FELDERA_PYTEST_OIDC_TOKEN")
    if env_token:
        try:
            import base64

            token_json = base64.b64decode(env_token.encode()).decode()
            token_data = json.loads(token_json)

            access_token = token_data.get("access_token")
            if (
                access_token is None
                or time.time() >= token_data.get("expires_at", 0) - 30
            ):
                raise RuntimeError("OIDC token expired before test execution")
            return access_token
        except Exception as e:
            raise RuntimeError(f"Failed to parse OIDC token from environment: {e}")

    return None
