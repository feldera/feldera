"""Manage a Feldera pipeline-manager server process.

On first start the server auto-deploys bundled data (binary, JAR,
toolchain, precompile cache) from the wheel to ``~/.pyfeldera/``.
No external compilers or runtimes are required on the host.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import requests

from pyfeldera._paths import _data_root, validate

logger = logging.getLogger("pyfeldera")

# The precompile cache was built with --dbsp-override-path=/home/ubuntu/feldera.
# To reuse it without recompilation, crate sources must live at this exact path.
_CACHE_COMPATIBLE_PATH = Path("/home/ubuntu/feldera")
_DEFAULT_DEPLOY_DIR = Path.home() / ".pyfeldera"


def _deploy_if_needed(deploy_dir: Path) -> Path:
    """Copy bundled data to *deploy_dir* if not already present.

    To keep the precompile cache valid the crate sources must be
    reachable at the same absolute path used when the cache was built
    (``/home/ubuntu/feldera``).  If the deploy dir differs, a symlink
    is created at that path pointing back to the deploy dir.
    """
    marker = deploy_dir / ".deployed"
    if marker.exists():
        logger.debug("Deploy dir %s already present, skipping copy", deploy_dir)
        return deploy_dir

    src = _data_root()
    if not src.exists():
        raise FileNotFoundError(
            f"Bundled data not found at {src}. Is the wheel built correctly?"
        )

    logger.info("First run — deploying pyfeldera to %s ...", deploy_dir)
    deploy_dir.mkdir(parents=True, exist_ok=True)

    for item in src.iterdir():
        dst = deploy_dir / item.name
        if dst.exists():
            if dst.is_dir():
                shutil.rmtree(dst)
            else:
                dst.unlink()
        if item.is_dir():
            shutil.copytree(item, dst, symlinks=True)
        else:
            shutil.copy2(item, dst)

    # Ensure executables have +x
    binary = deploy_dir / "bin" / "pipeline-manager"
    if binary.exists():
        binary.chmod(binary.stat().st_mode | 0o111)
    cargo_bin = deploy_dir / "toolchain" / "cargo-bin"
    if cargo_bin.exists():
        for f in cargo_bin.iterdir():
            if f.is_file():
                f.chmod(f.stat().st_mode | 0o111)

    # Create a symlink at the cache-compatible path so the precompile
    # cache's hardcoded absolute paths resolve correctly.
    if deploy_dir.resolve() != _CACHE_COMPATIBLE_PATH.resolve():
        try:
            _CACHE_COMPATIBLE_PATH.parent.mkdir(parents=True, exist_ok=True)
            if _CACHE_COMPATIBLE_PATH.is_symlink() or _CACHE_COMPATIBLE_PATH.exists():
                _CACHE_COMPATIBLE_PATH.unlink()
            _CACHE_COMPATIBLE_PATH.symlink_to(deploy_dir.resolve())
            logger.info("Symlinked %s → %s for cache compat",
                        _CACHE_COMPATIBLE_PATH, deploy_dir.resolve())
        except OSError:
            logger.warning("Could not create symlink at %s — "
                           "first pipeline compile may be slow",
                           _CACHE_COMPATIBLE_PATH)

    marker.touch()
    logger.info("Deploy complete (%s)", deploy_dir)
    return deploy_dir


def _toolchain_env(deploy_dir: Path) -> dict[str, str]:
    """Build an env dict that puts the bundled toolchain on PATH."""
    tc = deploy_dir / "toolchain"
    env = os.environ.copy()

    # Java 21 (SQL compiler JAR targets Java 19+)
    java_home = tc / "java"
    if java_home.exists():
        env["JAVA_HOME"] = str(java_home)

    # Rust — use the actual toolchain binaries directly (no rustup multiplexer)
    rust_bin = tc / "rustup" / "bin"
    if rust_bin.exists():
        # RUSTUP_TOOLCHAIN tells any stray rustup calls where to look
        env["RUSTUP_TOOLCHAIN"] = str(tc / "rustup")

    # Cargo registry — use the deployed cache
    cargo_cache = deploy_dir / "cache" / ".cargo"
    if cargo_cache.exists():
        env["CARGO_HOME"] = str(cargo_cache)

    # Mold linker
    mold_bin = tc / "mold" / "bin"
    if mold_bin.exists():
        env["RUSTFLAGS"] = (
            "-C link-arg=-fuse-ld=mold "
            "-C link-arg=-Wl,--compress-debug-sections=zlib"
        )

    # NOTE: Do NOT set LD_LIBRARY_PATH for the bundled glibc — it would
    # leak into child processes (embedded PostgreSQL, Java, etc.) and cause
    # symbol mismatch crashes.  Instead, the bundled ld.so is invoked with
    # --library-path in _build_cmd() for the pipeline-manager binary only.

    # PATH: bundled tools first
    path_parts = []
    if java_home.exists():
        path_parts.append(str(java_home / "bin"))
    # Use actual Rust toolchain binaries directly (not the rustup multiplexer)
    if rust_bin.exists():
        path_parts.append(str(rust_bin))
    if mold_bin.exists():
        path_parts.append(str(mold_bin))
    if path_parts:
        env["PATH"] = ":".join(path_parts) + ":" + env.get("PATH", "")

    env.setdefault("RUST_LOG", "info")
    env.setdefault("RUST_BACKTRACE", "1")
    return env


class FelderaServer:
    """Start, monitor, and stop a Feldera pipeline-manager.

    Parameters
    ----------
    bind_address : str
        IP to bind the HTTP server to.
    port : int
        HTTP port (default 8080).
    deploy_dir : Path | None
        Where to deploy bundled files.  Defaults to ``~/.pyfeldera``.
    extra_args : list[str] | None
        Extra CLI flags forwarded to pipeline-manager.
    """

    def __init__(
        self,
        *,
        bind_address: str = "127.0.0.1",
        port: int = 8080,
        deploy_dir: str | Path | None = None,
        extra_args: list[str] | None = None,
    ) -> None:
        self.bind_address = bind_address
        self.port = port
        self.deploy_dir = Path(deploy_dir) if deploy_dir else _DEFAULT_DEPLOY_DIR
        self.extra_args = extra_args or []
        self._process: subprocess.Popen | None = None
        self._log_file = None
        self._log_path: Path | None = None

    def _ensure_deployed(self) -> Path:
        return _deploy_if_needed(self.deploy_dir)

    def _build_cmd(self, home: Path, *, precompile: bool = False) -> list[str]:
        # Use the cache-compatible path for --dbsp-override-path so the
        # precompile cache's Cargo workspace fingerprints stay valid.
        override = _CACHE_COMPATIBLE_PATH if _CACHE_COMPATIBLE_PATH.exists() else home
        binary = str(home / "bin" / "pipeline-manager")

        # If a bundled glibc dynamic linker exists, use it with --library-path
        # so we don't need the host to have glibc >= 2.39.
        # Using --library-path instead of LD_LIBRARY_PATH ensures child
        # processes (embedded PostgreSQL, Java, etc.) use the host's glibc.
        ld_so = home / "toolchain" / "glibc" / "lib64" / "ld-linux-x86-64.so.2"
        glibc_lib = home / "toolchain" / "glibc" / "lib" / "x86_64-linux-gnu"
        if ld_so.exists() and glibc_lib.exists():
            cmd = [str(ld_so), "--library-path", str(glibc_lib), binary]
        else:
            cmd = [binary]

        cmd += [
            f"--bind-address={self.bind_address}",
            f"--sql-compiler-path={home / 'build' / 'sql2dbsp-jar-with-dependencies.jar'}",
            f"--compilation-cargo-lock-path={override / 'Cargo.lock'}",
            f"--dbsp-override-path={override}",
        ]
        # Point at the deployed precompile cache
        feldera_state = home / "cache" / ".feldera"
        if feldera_state.exists():
            cmd.append(f"--compiler-working-directory={feldera_state / 'compiler'}")
            cmd.append(f"--runner-working-directory={feldera_state / 'local-runner'}")
        if precompile:
            cmd.append("--precompile")
        cmd.extend(self.extra_args)
        return cmd

    @property
    def base_url(self) -> str:
        return f"http://{self.bind_address}:{self.port}"

    def precompile(self) -> None:
        """Run the precompile step (warms Rust dependency cache)."""
        home = self._ensure_deployed()
        cmd = self._build_cmd(home, precompile=True)
        env = _toolchain_env(home)
        logger.info("Precompiling: %s", " ".join(cmd))
        subprocess.run(cmd, env=env, check=True)

    def start(self) -> None:
        """Start the pipeline-manager in the background."""
        if self._process and self._process.poll() is None:
            raise RuntimeError("Server is already running")
        home = self._ensure_deployed()
        cmd = self._build_cmd(home)
        env = _toolchain_env(home)
        logger.info("Starting Feldera: %s", " ".join(cmd))

        # Log to file so output is available even from background threads
        self._log_path = home / "pipeline-manager.log"
        self._log_file = open(self._log_path, "w")
        self._process = subprocess.Popen(
            cmd, env=env,
            stdout=self._log_file, stderr=subprocess.STDOUT,
        )

    def wait_for_healthy(self, timeout: float = 120, interval: float = 2) -> None:
        """Block until ``/healthz`` responds 200."""
        url = f"{self.base_url}/healthz"
        deadline = time.monotonic() + timeout
        last_exc: Exception | None = None
        while time.monotonic() < deadline:
            # Check if the process crashed
            if self._process and self._process.poll() is not None:
                raise RuntimeError(
                    f"pipeline-manager exited with code {self._process.returncode}"
                )
            try:
                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    logger.info("Feldera healthy at %s", self.base_url)
                    return
            except requests.RequestException as exc:
                last_exc = exc
            time.sleep(interval)
        raise TimeoutError(f"Not healthy within {timeout}s (last: {last_exc})")

    def stop(self, timeout: float = 10) -> None:
        """SIGTERM then wait."""
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
            self._process.kill()
            self._process.wait(timeout=5)
        self._process = None
        if self._log_file:
            self._log_file.close()
            self._log_file = None

    def get_log(self, tail: int = 100) -> str:
        """Return the last *tail* lines of the pipeline-manager log."""
        if not self._log_path or not self._log_path.exists():
            return "(no log file)"
        lines = self._log_path.read_text().splitlines()
        return "\n".join(lines[-tail:])

    def start_blocking(self) -> None:
        """Start and block until exit or signal."""
        self.start()
        assert self._process is not None

        # Signal handlers only work in the main thread; skip if called
        # from a background thread (e.g. a Jupyter notebook cell).
        try:
            def _on_signal(signum, _frame):
                self.stop()
                sys.exit(0)

            signal.signal(signal.SIGTERM, _on_signal)
            signal.signal(signal.SIGINT, _on_signal)
        except ValueError:
            pass  # not in main thread — signals handled by caller

        self._process.wait()
