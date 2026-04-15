import enum
import logging
import subprocess
import time
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared constants for the Docker Compose integration-test stack
# ---------------------------------------------------------------------------

PROJECT_NAME = "feldera-dbt-test"
"""Docker Compose project name used to isolate integration-test containers."""


class Service(str, enum.Enum):
    """Docker Compose service names defined in ``docker-compose.yml``."""

    REDPANDA = "redpanda"
    PIPELINE_MANAGER = "pipeline-manager"
    DUCKDB = "duckdb"
    DELTA_INIT = "delta-init"


class DockerManager:
    """
    Manages Docker Compose lifecycle for integration tests.

    Wraps the ``docker compose`` CLI to start, stop, and health-check
    containers. Inspired by the Scala DockerClient trait at
    ``gh-pages/monitoring/projects/spark-orchestrator/common/``.
    """

    def __init__(
        self,
        compose_file: str,
        project_name: str = PROJECT_NAME,
    ) -> None:
        """
        Initialize the DockerManager.

        :param compose_file: Path to the docker-compose.yml file.
        :param project_name: Docker Compose project name for isolation.
        """
        self._compose_file = str(Path(compose_file).resolve())
        self._project_name = project_name

    def _run(self, args: List[str], check: bool = True, timeout: Optional[int] = None) -> subprocess.CompletedProcess:
        """
        Run a docker compose command.

        :param args: Arguments to pass to ``docker compose``.
        :param check: Whether to raise on non-zero exit code.
        :param timeout: Command timeout in seconds.
        :return: The CompletedProcess result.
        """
        cmd = [
            "docker",
            "compose",
            "-f",
            self._compose_file,
            "-p",
            self._project_name,
        ] + args

        logger.info("Running: %s", " ".join(cmd))
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        if result.stdout:
            logger.debug("stdout: %s", result.stdout[:500])
        if result.stderr:
            logger.debug("stderr: %s", result.stderr[:500])

        if check and result.returncode != 0:
            raise RuntimeError(
                f"Docker compose command failed (exit {result.returncode}):\n"
                f"cmd: {' '.join(cmd)}\n"
                f"stderr: {result.stderr[:1000]}"
            )

        return result

    def up(self, detach: bool = True, wait: bool = True, timeout: int = 300) -> None:
        """
        Start containers using docker compose up.

        :param detach: Run in detached mode.
        :param wait: Wait for containers to be healthy.
        :param timeout: Maximum wait time in seconds.
        """
        args = ["up"]
        if detach:
            args.append("-d")
        if wait:
            args.append("--wait")
            args.extend(["--wait-timeout", str(timeout)])

        self._run(args, timeout=timeout + 30)
        logger.info("Docker compose up completed")

    def down(self, volumes: bool = True, timeout: int = 60) -> None:
        """
        Stop and remove containers.

        :param volumes: Also remove volumes.
        :param timeout: Command timeout in seconds.
        """
        args = ["down"]
        if volumes:
            args.append("-v")
        args.append("--remove-orphans")

        self._run(args, check=False, timeout=timeout)
        logger.info("Docker compose down completed")

    def is_healthy(self, service: str = Service.PIPELINE_MANAGER) -> bool:
        """
        Check if a service container is healthy.

        :param service: The service name to check.
        :return: True if the container is healthy.
        """
        result = self._run(
            ["ps", "--format", "json", service],
            check=False,
        )
        return "healthy" in result.stdout.lower()

    def wait_for_healthy(
        self,
        service: str = Service.PIPELINE_MANAGER,
        timeout: int = 300,
        interval: int = 5,
    ) -> None:
        """
        Poll until a service is healthy.

        :param service: The service name to check.
        :param timeout: Maximum wait time in seconds.
        :param interval: Polling interval in seconds.
        :raises TimeoutError: If the service does not become healthy in time.
        """
        start = time.time()
        while time.time() - start < timeout:
            if self.is_healthy(service):
                logger.info("Service '%s' is healthy", service)
                return
            logger.debug("Waiting for '%s' to become healthy...", service)
            time.sleep(interval)
        raise TimeoutError(f"Service '{service}' did not become healthy within {timeout}s")

    def logs(self, service: Optional[str] = None, tail: int = 100) -> str:
        """
        Retrieve container logs.

        :param service: Optional service name (all services if None).
        :param tail: Number of trailing lines to return.
        :return: The log output as a string.
        """
        args = ["logs", "--tail", str(tail)]
        if service:
            args.append(service)
        result = self._run(args, check=False)
        return result.stdout + result.stderr
