import logging
import os
import sys
import urllib.request
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from docker_manager import DockerManager

logger = logging.getLogger(__name__)

COMPOSE_FILE = str(Path(__file__).parent / "docker-compose.yml")
FELDERA_URL = os.environ.get("FELDERA_URL", "http://localhost:8080")


def _resolve_feldera_url(base_url: str) -> str:
    """
    Probe for a reachable Feldera instance.

    In Docker-in-Docker environments (e.g., devcontainers), ``localhost``
    may not reach the Feldera container. This function tries the given
    URL first, then falls back to ``host.docker.internal`` if the base
    URL contains ``localhost`` or ``127.0.0.1``.

    :param base_url: The URL to try first.
    :return: The first reachable URL, or the original if none work.
    """
    candidates = [base_url]

    if "localhost" in base_url or "127.0.0.1" in base_url:
        alt = base_url.replace("localhost", "host.docker.internal").replace("127.0.0.1", "host.docker.internal")
        if alt != base_url:
            candidates.append(alt)

    for url in candidates:
        try:
            urllib.request.urlopen(f"{url}/v0/config", timeout=5)
            logger.info("Feldera reachable at %s", url)
            return url
        except Exception:
            logger.debug("Feldera not reachable at %s", url)

    logger.warning("Feldera not reachable at any candidate URL; using %s", base_url)
    return base_url


@pytest.fixture(scope="session")
def docker_feldera():
    """
    Session-scoped fixture that starts Feldera via Docker Compose
    and tears it down after all tests complete.

    Set FELDERA_SKIP_DOCKER=1 to skip Docker management (e.g., when
    Feldera is already running externally).
    """
    if os.environ.get("FELDERA_SKIP_DOCKER", "0") == "1":
        logger.info("Skipping Docker management (FELDERA_SKIP_DOCKER=1)")
        resolved = _resolve_feldera_url(FELDERA_URL)
        yield resolved
        return

    manager = DockerManager(compose_file=COMPOSE_FILE)

    try:
        logger.info("Starting Feldera via Docker Compose...")
        manager.down(volumes=True)
        manager.up(detach=True, wait=True, timeout=300)
        manager.wait_for_healthy(timeout=300)
        resolved = _resolve_feldera_url(FELDERA_URL)
        logger.info("Feldera is ready at %s", resolved)
        yield resolved
    finally:
        logger.info("Stopping Feldera...")
        logs = manager.logs(tail=50)
        logger.info("Feldera logs (last 50 lines):\n%s", logs)
        manager.down(volumes=True)


@pytest.fixture(scope="session")
def dbt_project_dir():
    """Return the path to the dbt-adventureworks project directory."""
    return str(Path(__file__).parent / "dbt-adventureworks")
