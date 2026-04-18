"""Basic integration tests for a pyfeldera-powered Feldera instance.

These tests run against a Docker Compose stack that simulates a customer
machine with only ``pip install pyfeldera`` and the required host toolchain.

Prerequisites:
    docker compose -f tests/docker/docker-compose.yml -p pyfeldera-test up -d --wait
"""

from __future__ import annotations

import requests


class TestFelderaHealth:
    """Verify the pipeline-manager is reachable and serving."""

    def test_healthz(self, feldera_url: str) -> None:
        """GET /healthz returns 200."""
        resp = requests.get(f"{feldera_url}/healthz", timeout=10)
        assert resp.status_code == 200

    def test_pipelines_endpoint(self, feldera_url: str) -> None:
        """GET /v0/pipelines returns 200 with a (possibly empty) list."""
        resp = requests.get(f"{feldera_url}/v0/pipelines", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_config_endpoint(self, feldera_url: str) -> None:
        """GET /v0/config returns valid JSON with expected keys."""
        resp = requests.get(f"{feldera_url}/v0/config", timeout=10)
        assert resp.status_code == 200
        data = resp.json()
        assert "version" in data

    def test_web_ui_root(self, feldera_url: str) -> None:
        """GET / returns HTML (the embedded Web UI)."""
        resp = requests.get(f"{feldera_url}/", timeout=10)
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")
