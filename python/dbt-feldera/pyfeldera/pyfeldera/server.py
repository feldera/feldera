"""Manage a Feldera pipeline-manager server process."""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import requests

from pyfeldera._paths import (
    cargo_lock,
    dbsp_override_path,
    pipeline_manager_bin,
    sql_compiler_jar,
    validate,
)

logger = logging.getLogger("pyfeldera")


class FelderaServer:
    """Start, monitor, and stop a Feldera pipeline-manager instance.

    Parameters
    ----------
    bind_address:
        IP address to bind the HTTP server to.
    port:
        Port for the HTTP server.
    working_dir:
        Base working directory for Feldera state.  Defaults to ``~/.feldera``.
    feldera_home:
        Override path for the Feldera source tree.  When ``None``,
        the bundled ``data/`` directory is used.
    extra_args:
        Additional CLI arguments forwarded to pipeline-manager.
    """

    def __init__(
        self,
        *,
        bind_address: str = "127.0.0.1",
        port: int = 8080,
        working_dir: str | Path | None = None,
        feldera_home: str | Path | None = None,
        extra_args: list[str] | None = None,
    ) -> None:
        self.bind_address = bind_address
        self.port = port
        self.working_dir = Path(working_dir) if working_dir else None
        self.feldera_home = Path(feldera_home) if feldera_home else None
        self.extra_args = extra_args or []
        self._process: subprocess.Popen | None = None

    # ── helpers ──────────────────────────────────────────────────────

    def _resolve_feldera_home(self) -> Path:
        if self.feldera_home:
            return self.feldera_home
        return dbsp_override_path()

    def _build_cmd(self, *, precompile: bool = False) -> list[str]:
        validate()

        home = self._resolve_feldera_home()
        jar = self.feldera_home / "build" / "sql2dbsp-jar-with-dependencies.jar" \
            if self.feldera_home else sql_compiler_jar()
        lock = self.feldera_home / "Cargo.lock" \
            if self.feldera_home else cargo_lock()
        binary = self.feldera_home / "bin" / "pipeline-manager" \
            if self.feldera_home else pipeline_manager_bin()

        cmd = [
            str(binary),
            f"--bind-address={self.bind_address}",
            f"--sql-compiler-path={jar}",
            f"--compilation-cargo-lock-path={lock}",
            f"--dbsp-override-path={home}",
        ]

        if self.working_dir:
            cmd.append(f"--compiler-working-directory={self.working_dir / 'compiler'}")
            cmd.append(f"--runner-working-directory={self.working_dir / 'local-runner'}")

        if precompile:
            cmd.append("--precompile")

        cmd.extend(self.extra_args)
        return cmd

    def _env(self) -> dict[str, str]:
        env = os.environ.copy()
        env.setdefault("RUST_LOG", "info")
        env.setdefault("RUST_BACKTRACE", "1")
        return env

    @property
    def base_url(self) -> str:
        return f"http://{self.bind_address}:{self.port}"

    # ── public API ───────────────────────────────────────────────────

    def precompile(self) -> None:
        """Run the precompile step, then exit.

        This warms the Rust dependency cache so subsequent pipeline
        compilations are fast.
        """
        cmd = self._build_cmd(precompile=True)
        logger.info("Precompiling: %s", " ".join(cmd))
        subprocess.run(cmd, env=self._env(), check=True)
        logger.info("Precompilation finished")

    def start(self) -> None:
        """Start the pipeline-manager in the background."""
        if self._process and self._process.poll() is None:
            raise RuntimeError("Server is already running")

        cmd = self._build_cmd()
        logger.info("Starting Feldera: %s", " ".join(cmd))
        self._process = subprocess.Popen(
            cmd,
            env=self._env(),
            stdout=sys.stdout,
            stderr=sys.stderr,
        )

    def wait_for_healthy(self, timeout: float = 120, interval: float = 2) -> None:
        """Block until the health endpoint responds 200, or raise."""
        url = f"{self.base_url}/healthz"
        deadline = time.monotonic() + timeout
        last_exc: Exception | None = None

        while time.monotonic() < deadline:
            try:
                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    logger.info("Feldera is healthy at %s", self.base_url)
                    return
            except requests.RequestException as exc:
                last_exc = exc
            time.sleep(interval)

        raise TimeoutError(
            f"Feldera at {url} not healthy within {timeout}s "
            f"(last error: {last_exc})"
        )

    def stop(self, timeout: float = 10) -> None:
        """Send SIGTERM and wait for graceful shutdown."""
        if not self._process:
            return
        if self._process.poll() is not None:
            self._process = None
            return

        logger.info("Stopping Feldera (pid=%d)", self._process.pid)
        self._process.send_signal(signal.SIGTERM)
        try:
            self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.warning("Forcefully killing Feldera")
            self._process.kill()
            self._process.wait(timeout=5)
        self._process = None

    def start_blocking(self) -> None:
        """Start and block until the process exits or a signal is caught."""
        self.start()
        assert self._process is not None

        def _handle_signal(signum, _frame):
            logger.info("Received signal %s, shutting down", signum)
            self.stop()
            sys.exit(0)

        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)

        self._process.wait()
