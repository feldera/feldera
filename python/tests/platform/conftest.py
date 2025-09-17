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
from typing import Optional


def is_master(config) -> bool:
    """True if the code running is in the xdist master node or not using xdist at all."""
    return not hasattr(config, 'workerinput')


def _fetch_oidc_token():
    """Fetch OIDC token using Resource Owner Password Grant flow."""
    from .oidc_test_helper import get_oidc_test_helper

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
            raise Exception(f"Token request failed: {response.status_code} - {response.text}")

        token_response = response.json()
        print("🔐 AUTH: ✅ Token request SUCCESS!")

        access_token = token_response["access_token"]
        expires_in = token_response.get("expires_in", 3600)
        expires_at = time.time() + expires_in

        return {
            'access_token': access_token,
            'expires_at': expires_at,
            'cached_at': time.time()
        }

    except Exception as e:
        print(f"🔐 AUTH: CRITICAL FAILURE - Failed to fetch OIDC token: {e}")
        raise RuntimeError(f"OIDC authentication is configured but token retrieval failed: {e}") from e


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
            os.environ['FELDERA_PYTEST_OIDC_TOKEN'] = token_b64
            print("🔐 AUTH: Token stored in environment for cross-process access")
        else:
            print("🔐 AUTH: No OIDC configuration found, using fallback authentication")
            config.oidc_token_data = None


def pytest_configure_node(node):
    """xdist hook: distribute OIDC token from master to worker nodes."""
    # Send the token data from master to worker
    token_data = getattr(node.config, 'oidc_token_data', None)
    if token_data:
        print(f"🔐 AUTH: Distributing token from master to worker {node.workerinput['workerid']}")
        node.workerinput['oidc_token_data'] = token_data
        
    else:
        node.workerinput['oidc_token_data'] = None


@pytest.fixture(scope='session', autouse=True)
def oidc_token_fixture(request):
    """
    Session-scoped fixture that sets up OIDC token in helper's cache.

    The actual token fetching is done by pytest_configure hooks to ensure
    it happens only once on the master node, then distributed to all workers.
    """
    from .oidc_test_helper import get_oidc_test_helper

    oidc_helper = get_oidc_test_helper()
    if oidc_helper is None:
        print("🔐 AUTH: No OIDC helper configured")
        return None

    # Get token data from master (via pytest_configure hooks) or worker input
    if is_master(request.config):
        # Master node: use token data from pytest_configure
        token_data = getattr(request.config, 'oidc_token_data', None)
    else:
        # Worker node: get token data sent from master
        token_data = request.config.workerinput.get('oidc_token_data')

    if token_data:
        # Set token in helper's in-memory cache for good measure
        oidc_helper._access_token = token_data['access_token']
        oidc_helper._token_expires_at = token_data['expires_at']

        # Verify token is not expired
        if time.time() < token_data['expires_at'] - 30:  # 30 second buffer
            print(f"🔐 AUTH: ✅ Token configured for {'master' if is_master(request.config) else f'worker {request.config.workerinput.get('workerid')}'}.")
            return token_data['access_token']
        else:
            print("🔐 AUTH: ⚠️ Token expired")
            raise RuntimeError("OIDC token expired before test execution")
    else:
        print("🔐 AUTH: No token data available (will use fallback authentication)")
        return None


@pytest.fixture(autouse=True)
def setup_oidc_auth(request, oidc_token_fixture):
    """
    Auto-used fixture that ensures OIDC authentication is set up for each test.

    This fixture runs for every test and ensures the OIDC token (if available)
    is properly configured in the helper functions. The actual token fetching
    and distribution is handled by the pytest_configure hooks above.
    """
    # The hooks and oidc_token_fixture have done all the token setup work
    pass