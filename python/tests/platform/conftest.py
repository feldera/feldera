"""
pytest configuration for platform tests.

Provides shared fixtures for OIDC authentication caching across pytest workers.
Uses pytest-xdist hooks to ensure OIDC token fetching happens only once on the master node.
"""

import os
import sys
from pathlib import Path

import pytest
import logging

# The one runtime besides the platform's own that the platform tests may run on.
GEN2_RUNTIME_VERSION = "gen2"


def is_master(config) -> bool:
    """True if the code running is in the xdist master node or not using xdist at all."""
    return not hasattr(config, "workerinput")


def refuse_unexpected_runtime_version():
    """
    `FELDERA_RUNTIME_VERSION` pins every pipeline the platform tests create.
    Any value but gen2 is most likely leaked from the runtime tests' CI job,
    which sets it to the commit SHA, so fail before running on the wrong runtime.
    """
    runtime_version = os.environ.get("FELDERA_RUNTIME_VERSION")
    if runtime_version and runtime_version != GEN2_RUNTIME_VERSION:
        raise pytest.UsageError(
            f"FELDERA_RUNTIME_VERSION is '{runtime_version}', but the platform tests "
            f"run on the platform's own runtime or on '{GEN2_RUNTIME_VERSION}'. "
            "Unset FELDERA_RUNTIME_VERSION to test the platform's own runtime."
        )


def pytest_configure(config):
    """Configure hook: refuse an unexpected runtime pin, fetch OIDC token on master node only."""
    refuse_unexpected_runtime_version()

    # Keep SDK debug logs enabled in tests without affecting production defaults.
    logging.getLogger("feldera.rest.feldera_client").setLevel(logging.DEBUG)

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


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(item, call):
    """Keeps each phase's report on the test item, so that a fixture can tell
    in its teardown whether the test failed (``item.report_call.failed``)."""
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"report_{report.when}", report)


@pytest.fixture(scope="session")
def run_shared_dir(tmp_path_factory, worker_id) -> Path:
    """A directory that all the processes of this test run share, and no
    other run.

    pytest-xdist runs the tests in several worker processes. What they must
    build only once, they coordinate through files in this directory. The
    base temporary directory of a worker is a subdirectory of the
    controller's, which is new for every run.
    """
    base = tmp_path_factory.getbasetemp()
    return base if worker_id == "master" else base.parent


def pytest_sessionfinish(session):
    """Removes what the processes of the run shared through `run_shared_dir`,
    after the last test of the run."""
    factory = getattr(session.config, "_tmp_path_factory", None)
    if not is_master(session.config) or factory is None:
        return
    shared = factory.getbasetemp()
    if not any(shared.glob("sync-golden-*.json")):
        return
    from tests.platform.checkpoint_sync_harness import remove_golden_buckets

    for failure in remove_golden_buckets(shared):
        print(f"cleanup: {failure}", file=sys.stderr)
