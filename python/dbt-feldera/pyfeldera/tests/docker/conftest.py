"""Fixtures for pyfeldera Docker tests."""

from __future__ import annotations

import pytest
import requests


def _resolve_url(port: int = 8089) -> str:
    """Try localhost, then host.docker.internal (Docker-in-Docker)."""
    for host in ["localhost", "host.docker.internal"]:
        url = f"http://{host}:{port}"
        try:
            resp = requests.get(f"{url}/healthz", timeout=5)
            if resp.status_code == 200:
                return url
        except requests.RequestException:
            continue
    raise RuntimeError(f"Feldera not reachable on port {port}")


@pytest.fixture(scope="session")
def feldera_url() -> str:
    """Base URL where the pyfeldera customer container is running."""
    return _resolve_url()
