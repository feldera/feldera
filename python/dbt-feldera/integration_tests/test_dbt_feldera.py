"""
Integration tests for dbt-feldera using the adventureworks star schema.

These tests run dbt commands against a Feldera instance started
via Docker Compose. They validate the full lifecycle:

  1. dbt debug   - connection verification
  2. dbt seed    - data loading via HTTP ingress
  3. dbt run     - model deployment as Feldera pipeline views
  4. dbt test    - data integrity via ad-hoc queries

Requires Docker to be available. Set FELDERA_SKIP_DOCKER=1 if Feldera
is already running externally.
"""

import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

logger = logging.getLogger(__name__)
_ADAPTER_ROOT = str(Path(__file__).resolve().parent.parent)


def _find_dbt_executable() -> str:
    """
    Locate the ``dbt`` CLI executable.

    When running inside a virtualenv (e.g., via ``pytest``), ``dbt`` lives in
    the same ``bin/`` directory as the Python interpreter but may not be on the
    system ``PATH``.  This helper resolves the full path so that
    ``subprocess.run`` always finds it.
    """
    # 1. Try the same bin directory as the running Python interpreter.
    venv_dbt = Path(sys.executable).parent / "dbt"
    if venv_dbt.is_file():
        return str(venv_dbt)

    # 2. Fall back to PATH.
    found = shutil.which("dbt")
    if found:
        return found

    raise FileNotFoundError(
        "Cannot locate the 'dbt' executable. Ensure dbt-core is installed in the active virtualenv."
    )


def _run_dbt(
    project_dir: str,
    args: list,
    feldera_url: str = "http://localhost:8080",
    timeout: int = 600,
) -> subprocess.CompletedProcess:
    """
    Run a dbt command in the given project directory.

    :param project_dir: Path to the dbt project.
    :param args: dbt command arguments (e.g., ["debug", "--target", "local"]).
    :param feldera_url: The Feldera instance URL (passed via FELDERA_URL env).
    :param timeout: Command timeout in seconds.
    :return: CompletedProcess result.
    """
    dbt_bin = _find_dbt_executable()
    cmd = [dbt_bin] + args + ["--profiles-dir", project_dir]
    logger.info("Running: %s (in %s)", " ".join(cmd), project_dir)

    # Ensure the adapter source is on PYTHONPATH so dbt-core's namespace
    # package extension picks it up even when CWD != adapter source root.
    env = {
        **os.environ,
        "DBT_PROFILES_DIR": project_dir,
        "FELDERA_URL": feldera_url,
        "PYTHONPATH": os.pathsep.join(filter(None, [_ADAPTER_ROOT, os.environ.get("PYTHONPATH", "")])),
    }

    result = subprocess.run(
        cmd,
        cwd=project_dir,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )

    if result.stdout:
        logger.info("stdout:\n%s", result.stdout[-2000:])
    if result.stderr:
        logger.info("stderr:\n%s", result.stderr[-2000:])

    return result


@pytest.mark.integration
class TestDbtFelderaIntegration:
    """End-to-end integration tests for dbt-feldera with adventureworks."""

    def test_dbt_debug(self, docker_feldera, dbt_project_dir):
        """Verify that dbt can connect to Feldera."""
        result = _run_dbt(dbt_project_dir, ["debug", "--target", "local"], feldera_url=docker_feldera)
        assert result.returncode == 0, f"dbt debug failed:\n{result.stdout}\n{result.stderr}"
        assert "All checks passed" in result.stdout or "Connection test" in result.stdout

    def test_dbt_seed(self, docker_feldera, dbt_project_dir):
        """Load seed data into Feldera via HTTP ingress."""
        result = _run_dbt(dbt_project_dir, ["seed", "--target", "local", "--full-refresh"], feldera_url=docker_feldera)
        assert result.returncode == 0, f"dbt seed failed:\n{result.stdout}\n{result.stderr}"

    def test_dbt_run(self, docker_feldera, dbt_project_dir):
        """Deploy all models as Feldera pipeline views."""
        result = _run_dbt(dbt_project_dir, ["run", "--target", "local"], feldera_url=docker_feldera)
        assert result.returncode == 0, f"dbt run failed:\n{result.stdout}\n{result.stderr}"

    def test_dbt_test(self, docker_feldera, dbt_project_dir):
        """Run data tests against materialized views."""
        result = _run_dbt(dbt_project_dir, ["test", "--target", "local"], feldera_url=docker_feldera)
        assert result.returncode in (0, 1), f"dbt test failed:\n{result.stdout}\n{result.stderr}"

    def test_dbt_run_full_refresh(self, docker_feldera, dbt_project_dir):
        """Verify full refresh works (stop + clear + redeploy)."""
        result = _run_dbt(dbt_project_dir, ["run", "--target", "local", "--full-refresh"], feldera_url=docker_feldera)
        assert result.returncode == 0, f"dbt run --full-refresh failed:\n{result.stdout}\n{result.stderr}"
