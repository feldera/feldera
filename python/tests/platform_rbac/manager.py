"""Pipeline-manager lifecycle for the RBAC suite.

The suite restarts the manager several times under different authentication
configurations and asserts that tenants, members and pipelines survive. That
requires two things the other platform suites do not need: control over when the
manager starts and stops, and state that outlives a restart.

State lives outside the manager process. The embedded Postgres data directory,
the compiler cache and the runner working directory sit in one directory tree
that every incarnation mounts, so replacing the manager keeps the database.

Two backends run the same manager. In CI `FELDERA_TEST_IMAGE` names a container
image; locally, `FELDERA_TEST_BINARY` (default `target/debug/pipeline-manager`)
runs the binary directly, which is what makes the suite debuggable without a
container runtime. Both speak the same flags, so the scenarios do not know which
one they are driving.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests
import urllib3

REPO_ROOT = Path(__file__).resolve().parents[3]

# The suite serves HTTPS with a certificate it generates for `localhost`, and
# points `verify=` at that certificate. urllib3 still warns about the self-signed
# chain on some platforms.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MANAGER_PORT = 8080
STARTUP_TIMEOUT_SECS = 180


@dataclass
class AuthConfig:
    """One authentication configuration the manager can boot under.

    `env` is merged into the manager's environment. `name` appears in test ids,
    so it should read as a scenario ("no-auth", "single-tenant", "multi-tenant").
    """

    name: str
    env: dict[str, str] = field(default_factory=dict)

    @property
    def is_authenticated(self) -> bool:
        return self.env.get("AUTH_PROVIDER", "none") != "none"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def generate_tls_cert(directory: Path) -> tuple[Path, Path, Path]:
    """A local CA, and the `localhost` server certificate it signs.

    Returns `(cert, key, ca_cert)`. Everything the suite runs over TLS serves
    `cert`: the manager's own listener and every issuer. The manager trusts
    `ca_cert` as a root, which is how it reaches those issuers.

    Two certificates rather than one self-signed: a certificate marked as a CA
    is refused when a server presents it (rustls calls this
    `CaUsedAsEndEntity`), so the root and the server certificate have to be
    different certificates.
    """
    directory.mkdir(parents=True, exist_ok=True)
    cert, key = directory / "tls.crt", directory / "tls.key"
    ca_cert, ca_key = directory / "ca.crt", directory / "ca.key"
    if cert.exists() and key.exists() and ca_cert.exists():
        return cert, key, ca_cert

    def openssl(*args: str) -> None:
        subprocess.run(["openssl", *args], check=True, capture_output=True)

    openssl(
        "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(ca_key), "-out", str(ca_cert),
        "-days", "365", "-subj", "/CN=Feldera RBAC test CA",
        "-addext", "basicConstraints=critical,CA:TRUE",
        "-addext", "keyUsage=critical,keyCertSign,cRLSign",
    )  # fmt: skip

    ext = directory / "x509_v3.ext"
    ext.write_text(
        "subjectAltName = @alt_names\n"
        "basicConstraints = critical, CA:FALSE\n"
        "keyUsage = critical, digitalSignature, keyEncipherment\n"
        "extendedKeyUsage = serverAuth\n\n"
        "[alt_names]\nDNS.1 = localhost\nIP.1 = 127.0.0.1\n"
    )
    csr = directory / "tls.csr"
    openssl(
        "req", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(key), "-out", str(csr), "-subj", "/CN=localhost",
    )  # fmt: skip
    openssl(
        "x509", "-req", "-in", str(csr),
        "-CA", str(ca_cert), "-CAkey", str(ca_key), "-CAcreateserial",
        "-out", str(cert), "-days", "365", "-extfile", str(ext),
    )  # fmt: skip
    key.chmod(0o644)
    return cert, key, ca_cert


class Manager:
    """A pipeline-manager the test can stop and restart under a new auth config.

    `state_dir` survives across restarts and holds the database; `run_dir` holds
    per-incarnation artifacts (TLS material, logs).
    """

    def __init__(self, state_dir: Path, run_dir: Path, https: bool = True):
        self.state_dir = state_dir
        self.run_dir = run_dir
        self.https = https
        self.image = os.environ.get("FELDERA_TEST_IMAGE")
        # Relative paths resolve against the repo root, not pytest's rootdir.
        binary = Path(
            os.environ.get("FELDERA_TEST_BINARY", "target/debug/pipeline-manager")
        )
        self.binary = str(binary if binary.is_absolute() else REPO_ROOT / binary)
        self.port = MANAGER_PORT if self.image else free_port()
        # The manager also binds a compiler and a runner port. In-container it
        # owns the whole network namespace and the defaults are fine; run
        # directly, a lingering process from an earlier run would collide.
        self._extra_ports = (
            []
            if self.image
            else [f"--compiler-port={free_port()}", f"--runner-port={free_port()}"]
        )
        self._proc: subprocess.Popen | None = None
        self._container: str | None = None
        # The container writes its state into a named volume rather than a bind
        # mount. Docker seeds a volume with the image's own ownership, so the
        # manager can write it as the user the image runs as; a host directory
        # would be owned by whoever ran pytest, and matching that with `--user`
        # costs the container access to its own installed files.
        self._volume = f"feldera-rbac-state-{os.getpid()}" if self.image else None
        self._volume_ready = False
        self.config: AuthConfig | None = None
        self.log_path = run_dir / "manager.log"

        for sub in ("pg", "runner", "compiler"):
            (state_dir / sub).mkdir(parents=True, exist_ok=True)
        run_dir.mkdir(parents=True, exist_ok=True)
        self.cert, self.key, self.ca_cert = (
            generate_tls_cert(run_dir / "tls") if https else (None, None, None)
        )

    @property
    def base_url(self) -> str:
        scheme = "https" if self.https else "http"
        return f"{scheme}://localhost:{self.port}"

    @property
    def verify(self):
        """`verify=` for requests: the CA that signed our certificate."""
        return str(self.ca_cert) if self.https else True

    # Where the state root is visible to the manager: a fixed path inside the
    # container, or the host directory when running the binary directly.
    CONTAINER_STATE = "/state"

    # Root stores a Linux image is likely to ship, in preference order.
    SYSTEM_CA_BUNDLES = (
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
    )

    def _ca_bundle(self) -> Path:
        """Roots the manager trusts: the suite's certificate and the platform's.

        The manager reaches every issuer over https, so it has to trust the CA
        that signed what they serve. Its TLS stack reads `SSL_CERT_FILE`
        *instead of* the system store rather than in addition to it, so the
        platform's roots are carried along; without them the manager loses every
        other outbound https destination.
        """
        bundle = self.run_dir / "ca-bundle.crt"
        roots = [self.ca_cert.read_text()]
        for path in map(Path, self.SYSTEM_CA_BUNDLES):
            if path.exists():
                roots.append(path.read_text())
                break
        bundle.write_text("\n".join(roots))
        return bundle

    def _flags(self) -> list[str]:
        root = self.CONTAINER_STATE if self.image else str(self.state_dir)
        flags = [
            f"--pg-embed-working-directory={root}/pg",
            f"--runner-working-directory={root}/runner",
            f"--compiler-working-directory={root}/compiler",
        ]
        if self.https:
            flags += [
                "--enable-https",
                f"--https-tls-cert-path={self.cert}",
                f"--https-tls-key-path={self.key}",
            ]
        return flags

    def start(self, config: AuthConfig) -> None:
        """Boot under `config` and block until the manager answers."""
        assert self._proc is None and self._container is None, "already running"
        self.config = config
        env = {
            # Why a token was refused is logged at debug in `auth` and `oidc`,
            # so at plain info a 401 reaches the test with no reason attached.
            "RUST_LOG": "info,pipeline_manager::auth=debug,pipeline_manager::oidc=debug",
            "RUST_BACKTRACE": "1",
            "FELDERA_UNSTABLE_FEATURES": "runtime_version,testing",
            "SSL_CERT_FILE": str(self._ca_bundle()),
            **config.env,
        }
        if self.image:
            self._start_container(env)
        else:
            self._start_process(env)
        self._await_healthy()

    def _start_process(self, env: dict[str, str]) -> None:
        log = self.log_path.open("ab")
        self._proc = subprocess.Popen(
            [
                self.binary,
                "--bind-address=127.0.0.1",
                f"--api-port={self.port}",
                *self._extra_ports,
                *self._flags(),
            ],
            env={**os.environ, **env},
            # The manager resolves its default SQL-compiler and cargo-lock paths
            # relative to the working directory, and pytest runs from `python/`.
            # The container image passes those as absolute paths, so this only
            # matters for the local backend.
            cwd=REPO_ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

    def _prepare_volume(self) -> None:
        """Create the state volume owned by the user the image runs as.

        Docker seeds a fresh volume from whatever the mount point holds in the
        image, and takes its ownership from there. `/state` does not exist in
        the image, so the volume is created root-owned and the manager, which
        does not run as root, cannot write it. Handing ownership over once, from
        a throwaway root container, is what makes it writable without touching
        the image or the host.
        """
        if self._volume_ready:
            return
        subprocess.run(
            ["docker", "volume", "create", self._volume],
            check=True,
            capture_output=True,
        )
        # Ask the image who it runs as rather than assuming a uid.
        owner = subprocess.run(
            ["docker", "run", "--rm", "--entrypoint", "id", self.image, "-u"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        state = self.CONTAINER_STATE
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--user",
                "0:0",
                "--entrypoint",
                "sh",
                "-v",
                f"{self._volume}:{state}",
                self.image,
                "-c",
                (
                    f"mkdir -p {state}/pg {state}/runner {state}/compiler "
                    f"&& chown -R {owner} {state}"
                ),
            ],
            check=True,
            capture_output=True,
        )
        self._volume_ready = True

    def _start_container(self, env: dict[str, str]) -> None:
        self._prepare_volume()
        self._container = f"feldera-rbac-{int(time.time() * 1000)}"
        env_args = []
        for k, v in env.items():
            env_args += ["-e", f"{k}={v}"]
        # `--network host` so the manager reaches the dummy issuer on localhost
        # for discovery and JWKS, the same way the previous CI job did.
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self._container,
                "--network",
                "host",
                "--pull",
                "missing",
                "-v",
                f"{self._volume}:{self.CONTAINER_STATE}",
                "--mount",
                f"type=bind,src={self.run_dir},dst={self.run_dir},readonly",
                *env_args,
                self.image,
                *self._flags(),
            ],
            check=True,
            capture_output=True,
        )

    def _await_healthy(self) -> None:
        deadline = time.time() + STARTUP_TIMEOUT_SECS
        last = ""
        while time.time() < deadline:
            if self._proc is not None and self._proc.poll() is not None:
                raise RuntimeError(f"manager exited early:\n{self.failure_summary()}")
            # A container that dies keeps answering "connection refused" until
            # the deadline, so without this the only evidence of what went wrong
            # is a timeout that names nothing.
            if self._container is not None and not self._container_running():
                raise RuntimeError(
                    f"manager container exited early:\n{self.failure_summary()}"
                )
            try:
                r = requests.get(
                    f"{self.base_url}/healthz", timeout=2, verify=self.verify
                )
                if r.status_code == 200:
                    return
                last = f"HTTP {r.status_code}"
            except requests.RequestException as e:
                last = str(e)
            time.sleep(1)
        raise RuntimeError(
            f"manager did not become healthy in {STARTUP_TIMEOUT_SECS}s "
            f"(last: {last})\n{self.failure_summary()}"
        )

    def _container_running(self) -> bool:
        out = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", self._container],
            capture_output=True,
            text=True,
            check=False,
        )
        return out.stdout.strip() == "true"

    def failure_summary(self) -> str:
        """The lines worth reading when the manager will not come up.

        A panic prints its message first and thirty frames of backtrace after,
        so the tail of the log is the least informative part of it.
        """
        interesting = [
            line
            for line in self.logs().splitlines()
            if any(
                marker in line
                for marker in (
                    "panicked",
                    "ERROR",
                    "error:",
                    "Error:",
                    "Missing environment",
                )
            )
        ]
        return "\n".join(interesting[-20:]) or self.logs()[-2000:]

    def logs(self) -> str:
        if self._container:
            out = subprocess.run(
                ["docker", "logs", self._container],
                capture_output=True,
                text=True,
                check=False,
            )
            return out.stdout + out.stderr
        return (
            self.log_path.read_text(errors="replace") if self.log_path.exists() else ""
        )

    def stop(self) -> None:
        if self._container:
            subprocess.run(
                ["docker", "rm", "-f", self._container],
                capture_output=True,
                check=False,
            )
            self._container = None
        if self._proc:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=10)
            self._proc = None
        self.config = None

    def restart(self, config: AuthConfig) -> None:
        """Swap the auth configuration without touching `state_dir`.

        This is the operation the scenarios are built around: everything the
        manager persisted must still be there afterwards.
        """
        self.stop()
        self.start(config)

    def remove_volume(self) -> None:
        """Discard the container's state. Restarts must not call this: keeping
        the volume across them is what the scenarios are testing."""
        if self._volume:
            self._volume_ready = False
            subprocess.run(
                ["docker", "volume", "rm", "-f", self._volume],
                capture_output=True,
                check=False,
            )

    def reset_state(self) -> None:
        assert self._proc is None and self._container is None, "stop the manager first"
        shutil.rmtree(self.state_dir, ignore_errors=True)
        for sub in ("pg", "runner", "compiler"):
            (self.state_dir / sub).mkdir(parents=True, exist_ok=True)
