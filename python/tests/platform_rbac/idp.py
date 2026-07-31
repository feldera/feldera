"""Identity providers for the RBAC suite.

Two issuers run side by side. The primary one backs every configuration the
manager boots under. The second is a *rogue* issuer: same shape, different
signing key and a different `iss`, which is what makes "wrong key" and "wrong
issuer" testable rather than assumed.

Tokens come from `scripts/dummy_oidc.py`, whose `/token` endpoint takes the
claims directly (`sub`, `aud`, `tenants`, `exp_secs`). A negative `exp_secs`
yields an already-expired token, so expiry needs no clock manipulation.
"""

from __future__ import annotations

import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parents[3]
DUMMY_OIDC = REPO_ROOT / "scripts" / "dummy_oidc.py"
DEFAULT_AUDIENCE = "feldera-api"


@dataclass
class Issuer:
    """A running dummy OIDC issuer."""

    url: str
    process: subprocess.Popen
    log_path: Path

    def token(
        self,
        subject: str,
        *,
        email: str | None = None,
        tenants: list[str] | None = None,
        audience: str | None = DEFAULT_AUDIENCE,
        expires_in: int = 3600,
    ) -> str:
        """Mint an access token asserting these claims.

        `expires_in` is passed through verbatim, so a negative value produces a
        token that is already expired.
        """
        params: dict[str, str] = {"sub": subject, "exp_secs": str(expires_in)}
        if email is not None:
            params["email"] = email
        if audience is not None:
            params["aud"] = audience
        if tenants:
            params["tenants"] = ",".join(tenants)
        r = requests.get(f"{self.url}/token", params=params, timeout=10)
        r.raise_for_status()
        return r.json()["access_token"]

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()


def start_issuer(port: int, run_dir: Path, name: str = "idp") -> Issuer:
    """Start `dummy_oidc.py` on `port` and wait for its discovery document."""
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / f"{name}.log"
    url = f"http://localhost:{port}"
    log = log_path.open("ab")
    process = subprocess.Popen(
        ["uv", "run", str(DUMMY_OIDC), "--port", str(port), "--issuer", url],
        cwd=REPO_ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    deadline = time.time() + 60
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                f"{name} exited early:\n{log_path.read_text(errors='replace')}"
            )
        try:
            r = requests.get(f"{url}/.well-known/openid-configuration", timeout=2)
            if r.status_code == 200:
                return Issuer(url=url, process=process, log_path=log_path)
        except requests.RequestException:
            pass
        time.sleep(0.5)
    process.kill()
    raise RuntimeError(
        f"{name} did not come up:\n{log_path.read_text(errors='replace')}"
    )
