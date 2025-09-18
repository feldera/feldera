"""
pytest configuration for platform tests.

Provides shared fixtures for OIDC authentication caching across pytest workers.
Uses pytest-xdist hooks to ensure OIDC token fetching happens only once on the master node.
"""

import pytest
import os


def is_master(config) -> bool:
    """True if the code running is in the xdist master node or not using xdist at all."""
    return not hasattr(config, "workerinput")




def pytest_configure(config):
    """Configure hook: fetch OIDC token on master node only."""
    if is_master(config):
        # This runs only on the master node (or in single-node mode)
        from feldera.testutils_oidc import setup_token_cache
        token_data = setup_token_cache()
        if token_data:
            print("🔐 AUTH: Master node cached OIDC token for distribution to workers")
            # Store token data in config for distribution to workers
            config.oidc_token_data = token_data
        else:
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
    from feldera.testutils_oidc import get_cached_token_from_env
    
    # Token is accessed via environment variable - this fixture just verifies setup
    token_data = get_cached_token_from_env()
    if token_data:
        return token_data.get("access_token")

    return None
