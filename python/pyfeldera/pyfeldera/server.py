"""Manage a Feldera pipeline-manager server process.

On first import the module auto-deploys bundled data (binary, JAR,
toolchain, precompile cache) from the wheel to ``~/.pyfeldera/``.
No external compilers or runtimes are required on the host.

The eager deploy on import is intentional: callers (e.g. Fabric notebooks)
may delete the package's ``data/`` directory after import to reclaim disk
space.  By deploying at import time the data is safely copied before any
caller-side cleanup can remove it.
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

# The precompile cache is re-built during extraction with
# --dbsp-override-path=/tmp/feldera so it works on any host.  /tmp is
# world-writable, unlike /home/ubuntu which requires root on Fabric VMs.
_CACHE_COMPATIBLE_PATH = Path("/tmp/feldera")

def _pick_default_deploy_dir() -> Path:
    """Choose the best default deploy directory.

    Prefers ``/mnt/.pyfeldera`` when ``/mnt`` exists and is writable —
    Fabric VMs expose a large scratch volume there (~58 GB) while the
    root filesystem has limited free space after pip install.
    Falls back to ``~/.pyfeldera`` on regular hosts.
    """
    env = os.environ.get("PYFELDERA_DEPLOY_DIR")
    if env:
        return Path(env)
    mnt = Path("/mnt")
    if mnt.is_dir() and os.access(mnt, os.W_OK):
        return mnt / ".pyfeldera"
    return Path.home() / ".pyfeldera"


_DEFAULT_DEPLOY_DIR = _pick_default_deploy_dir()

# Name of the file written during ``run.sh build-wheel`` to identify a
# particular wheel build.  Compared against the deployed copy to detect
# when ``pip install --force-reinstall`` delivers a newer wheel.
_BUILD_ID_FILE = ".build_id"


def _needs_redeploy(deploy_dir: Path, src: Path) -> bool:
    """Return *True* when the deploy directory is stale or incomplete.

    Checks the ``.build_id`` stamp written by ``run.sh build-wheel``.
    If the source build ID differs from the deployed one — or if the
    pipeline-manager binary is missing — a (re)deploy is required.
    """
    marker = deploy_dir / ".deployed"
    binary = deploy_dir / "bin" / "pipeline-manager"

    if not marker.exists() or not binary.exists():
        return True

    src_id_file = src / _BUILD_ID_FILE
    dst_id_file = deploy_dir / _BUILD_ID_FILE
    if src_id_file.exists():
        src_id = src_id_file.read_text().strip()
        dst_id = dst_id_file.read_text().strip() if dst_id_file.exists() else ""
        if src_id != dst_id:
            logger.info("Build ID mismatch (src=%s, dst=%s) — redeploying",
                        src_id[:12], dst_id[:12])
            return True

    return False


def _copy_workspace_sources(deploy_dir: Path, target: Path) -> None:
    """Copy workspace source files to *target* for fingerprint compat.

    The precompile cache was built with ``--dbsp-override-path=<target>``.
    Cargo resolves symlinks, so the source files must exist as real files
    at *target* (not via symlink).  Only the small workspace metadata and
    crate sources (~23 MB) are copied — the heavy toolchain and cache
    stay in *deploy_dir*.
    """
    if deploy_dir.resolve() == target.resolve():
        return  # nothing to do

    _WORKSPACE_ITEMS = [
        "Cargo.toml", "Cargo.lock", "README.md",
        "crates", "sql-to-dbsp-compiler",
    ]

    try:
        target.mkdir(parents=True, exist_ok=True)
        for name in _WORKSPACE_ITEMS:
            src = deploy_dir / name
            dst = target / name
            if not src.exists():
                continue
            if dst.is_symlink() or (dst.exists() and not dst.is_dir()):
                dst.unlink()
            elif dst.is_dir():
                shutil.rmtree(dst)
            if src.is_dir():
                shutil.copytree(src, dst, symlinks=True)
            else:
                shutil.copy2(src, dst)
        logger.info("Copied workspace sources to %s for cache compat", target)
    except OSError:
        logger.warning(
            "Could not copy workspace to %s -- "
            "first pipeline compile may be slow", target,
        )


def _deploy_if_needed(deploy_dir: Path) -> Path:
    """Copy bundled data to *deploy_dir* if not already present.

    To keep the precompile cache valid the crate sources must be
    reachable at the same absolute path used when the cache was built
    (``/home/ubuntu/feldera``).  If the deploy dir differs, a symlink
    is created at that path pointing back to the deploy dir.
    """
    src = _data_root()

    # Fast path — already deployed and up-to-date.
    if not _needs_redeploy(deploy_dir, src):
        logger.debug("Deploy dir %s up-to-date, skipping copy", deploy_dir)
        _ensure_cargo_wrapper(deploy_dir)
        return deploy_dir

    if not src.exists() or not any(src.iterdir()):
        # Source data was already cleaned (e.g. notebook disk cleanup).
        # If a prior deploy left a working binary, trust it.
        binary = deploy_dir / "bin" / "pipeline-manager"
        if binary.exists():
            logger.debug("Source data absent but deploy dir has binary — reusing")
            marker = deploy_dir / ".deployed"
            if not marker.exists():
                marker.touch()
            _ensure_cargo_wrapper(deploy_dir)
            return deploy_dir
        raise FileNotFoundError(
            f"Bundled data not found at {src} and no prior deploy at "
            f"{deploy_dir}. Is the wheel built correctly?"
        )

    logger.info("Deploying pyfeldera to %s ...", deploy_dir)
    deploy_dir.mkdir(parents=True, exist_ok=True)

    for item in src.iterdir():
        dst = deploy_dir / item.name
        # Remove existing destination — handle symlinks (from prior
        # lakehouse redirects) before checking is_dir() to avoid
        # following them into the lakehouse mount.
        if dst.is_symlink():
            dst.unlink()
        elif dst.exists():
            if dst.is_dir():
                shutil.rmtree(dst)
            else:
                dst.unlink()
        # Use move instead of copy — on the same filesystem (root) this
        # is an instant rename that avoids doubling the ~6 GB data footprint.
        # This keeps peak disk usage manageable on constrained Fabric VMs.
        shutil.move(str(item), str(dst))

    # Ensure executables have +x
    binary = deploy_dir / "bin" / "pipeline-manager"
    if binary.exists():
        binary.chmod(binary.stat().st_mode | 0o111)
    cargo_bin = deploy_dir / "toolchain" / "cargo-bin"
    if cargo_bin.exists():
        for f in cargo_bin.iterdir():
            if f.is_file():
                f.chmod(f.stat().st_mode | 0o111)

    # Copy workspace source files to the cache-compatible path so the
    # precompile cache's Cargo fingerprints resolve correctly.  Cargo
    # resolves symlinks, so a real copy (not symlink) is required.
    # Only the small source files (~23 MB) are copied, not the full
    # deploy directory.
    _copy_workspace_sources(deploy_dir, _CACHE_COMPATIBLE_PATH)

    (deploy_dir / ".deployed").touch()
    _ensure_cargo_wrapper(deploy_dir)

    # Free disk: remove any leftover source data in site-packages.
    # After ``shutil.move`` above this is normally empty, but clean up
    # defensively in case any items couldn't be moved.
    try:
        for item in src.iterdir():
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)
        logger.info("Cleaned source data at %s to free disk", src)
    except OSError:
        pass  # best-effort

    # Free disk: remove pip's HTTP/wheel cache — on constrained
    # environments the cached .whl can waste several GB.
    pip_cache = Path.home() / ".cache" / "pip"
    if pip_cache.exists():
        shutil.rmtree(pip_cache, ignore_errors=True)
        logger.info("Cleaned pip cache at %s to free disk", pip_cache)

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

    # libclang for bindgen (used during pipeline Rust compilation).
    # Set LIBCLANG_PATH so bindgen can find libclang.so.
    libclang_dir = tc / "libclang"
    if libclang_dir.exists():
        env["LIBCLANG_PATH"] = str(libclang_dir)

    # NOTE: Do NOT set LD_LIBRARY_PATH for the bundled glibc — it would
    # leak into child processes (embedded PostgreSQL, Java, etc.) and cause
    # symbol mismatch crashes.  Instead, the bundled ld.so is invoked with
    # --library-path in _build_cmd() for the pipeline-manager binary only.
    #
    # For libclang/LLVM deps, a cargo wrapper script sets LD_LIBRARY_PATH
    # only for cargo/rustc processes (see _ensure_cargo_wrapper).

    # PATH: bundled tools first
    path_parts = []
    if java_home.exists():
        path_parts.append(str(java_home / "bin"))
    # Cargo wrapper first so it intercepts `cargo` invocations
    cargo_wrapper_dir = tc / "cargo-wrapper"
    if cargo_wrapper_dir.exists():
        path_parts.append(str(cargo_wrapper_dir))
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


def _ensure_soname_symlinks(lib_dir: Path) -> None:
    """Ensure versioned shared-library files exist for runtime loading.

    During extraction ``tar -h`` dereferences symlinks, so the real
    file lands as ``libfoo.so`` but its embedded SONAME is
    ``libfoo.so.X``.  The dynamic linker looks up the SONAME file at
    runtime.  Normally the extraction script (``run.sh``) creates
    versioned copies, but this is a belt-and-suspenders fallback.
    """
    _KNOWN_SONAMES: dict[str, str] = {
        "libsasl2.so": "libsasl2.so.2",
    }
    for basename, soname in _KNOWN_SONAMES.items():
        so_file = lib_dir / basename
        versioned = lib_dir / soname
        if so_file.exists() and not versioned.exists():
            try:
                versioned.symlink_to(basename)
                logger.info("Created symlink %s → %s", soname, basename)
            except OSError:
                # Symlinks may fail on some filesystems; copy instead.
                shutil.copy2(so_file, versioned)
                logger.info("Copied %s as %s", basename, soname)


def _ensure_cargo_wrapper(deploy_dir: Path) -> None:
    """Create a wrapper script that sets env vars for cargo builds.

    Sets ``LIBCLANG_PATH`` + ``LD_LIBRARY_PATH`` for bundled libclang/LLVM,
    ``PKG_CONFIG_PATH`` + ``C_INCLUDE_PATH`` + ``LIBRARY_PATH`` for bundled
    dev headers (sasl2, ssl), ``BINDGEN_EXTRA_CLANG_ARGS`` for clang
    resource headers, and ``RUSTFLAGS`` rpath so compiled pipeline
    binaries find sysroot shared libraries at runtime.

    This keeps these env vars isolated to cargo processes — they do not
    leak into embedded PostgreSQL or Java, avoiding symbol mismatch crashes.
    """
    tc = deploy_dir / "toolchain"
    libclang_dir = tc / "libclang"
    rust_bin = tc / "rustup" / "bin"
    sysroot = tc / "sysroot"
    wrapper_dir = tc / "cargo-wrapper"

    if not rust_bin.exists():
        return

    wrapper_dir.mkdir(parents=True, exist_ok=True)
    wrapper = wrapper_dir / "cargo"

    # Fix sysroot pkg-config files to point to actual deploy paths
    if sysroot.exists():
        pc_dir = sysroot / "lib" / "pkgconfig"
        if pc_dir.exists():
            for pc_file in pc_dir.glob("*.pc"):
                content = pc_file.read_text()
                content = content.replace("SYSROOT_PREFIX", str(sysroot))
                content = content.replace("SYSROOT_LIB", str(sysroot / "lib"))
                content = content.replace("SYSROOT_INCLUDE", str(sysroot / "include"))
                pc_file.write_text(content)

    # Ensure versioned .so symlinks exist for runtime loading
    sysroot_lib = sysroot / "lib"
    if sysroot_lib.exists():
        _ensure_soname_symlinks(sysroot_lib)

    # Build wrapper script
    real_cargo = rust_bin / "cargo"
    lines = ["#!/bin/sh"]
    if libclang_dir.exists():
        clang_include = libclang_dir / "clang" / "18" / "include"
        lines.append(f'export LIBCLANG_PATH="{libclang_dir}"')
        lines.append(f'export LD_LIBRARY_PATH="{libclang_dir}:${{LD_LIBRARY_PATH:-}}"')
        lines.append(f'export BINDGEN_EXTRA_CLANG_ARGS="-isystem {clang_include}"')
    # Disable incremental compilation and debug info to reduce disk
    # usage during pipeline compilation.  On constrained Fabric VMs
    # (~59 GB root) every GB counts.
    lines.append('export CARGO_INCREMENTAL=0')
    if sysroot.exists():
        pc_dir = sysroot / "lib" / "pkgconfig"
        lines.append(f'export PKG_CONFIG_PATH="{pc_dir}:${{PKG_CONFIG_PATH:-}}"')
        lines.append(f'export C_INCLUDE_PATH="{sysroot / "include"}:${{C_INCLUDE_PATH:-}}"')
        lines.append(f'export LIBRARY_PATH="{sysroot_lib}:${{LIBRARY_PATH:-}}"')
        # Embed rpath so compiled pipeline binaries find sysroot libs at
        # runtime without requiring LD_LIBRARY_PATH (which would leak into
        # child processes like embedded PostgreSQL).
        lines.append(
            f'export RUSTFLAGS="${{RUSTFLAGS:-}}'
            f' -C debuginfo=0'
            f' -C link-arg=-Wl,-rpath,{sysroot_lib}"'
        )
    lines.append(f'exec "{real_cargo}" "$@"')

    wrapper.write_text("\n".join(lines) + "\n")
    wrapper.chmod(0o755)


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
        self._startup_error: Exception | None = None

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
        # Point at the compiler/runner working directories.
        # Always create them — the precompile cache may have been deleted
        # to free disk on constrained hosts.
        feldera_state = home / "cache" / ".feldera"
        compiler_dir = feldera_state / "compiler"
        runner_dir = feldera_state / "local-runner"
        compiler_dir.mkdir(parents=True, exist_ok=True)
        runner_dir.mkdir(parents=True, exist_ok=True)
        cmd.append(f"--compiler-working-directory={compiler_dir}")
        cmd.append(f"--runner-working-directory={runner_dir}")
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
            # Surface startup failures from background threads immediately.
            if self._startup_error is not None:
                raise RuntimeError(
                    f"Server failed to start: {self._startup_error}"
                ) from self._startup_error
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
        """Start and block until exit or signal.

        If an exception occurs during startup (deploy or process launch),
        it is stored in ``_startup_error`` so that ``wait_for_healthy``
        can surface a clear message instead of an opaque timeout.
        """
        try:
            self.start()
        except Exception as exc:
            self._startup_error = exc
            logger.error("start_blocking failed: %s", exc)
            raise

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


# ---------------------------------------------------------------------------
# Eager deploy on module import
# ---------------------------------------------------------------------------
# Trigger the deploy as soon as this module is loaded (typically via
# ``from pyfeldera.server import FelderaServer``).  This ensures the
# bundled data is safely copied to ``~/.pyfeldera/`` *before* any
# caller-side code can delete the package's ``data/`` directory to
# reclaim disk space.
#
# On the fast path (already deployed and up-to-date) this is two stat()
# calls — negligible overhead.
try:
    _deploy_if_needed(_DEFAULT_DEPLOY_DIR)
except FileNotFoundError:
    # Running from a source checkout or partial install — no bundled data.
    logger.debug("Eager deploy skipped: no bundled data available")
except Exception:
    logger.debug("Eager deploy skipped", exc_info=True)
