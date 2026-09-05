"""Identity providers for the RBAC suite.

Two issuers run side by side. The primary one backs every configuration the
manager boots under. The second is a *rogue* issuer: same shape, different
signing key and a different `iss`, which is what makes "wrong key" and "wrong
issuer" testable rather than assumed.

Tokens come from `scripts/dummy_oidc.py`, whose `/token` endpoint takes the
claims directly (`sub`, `aud`, `tenants`, `exp_secs`). A negative `exp_secs`
yields an already-expired token, so expiry needs no clock manipulation.

Every issuer serves https, because a trust registered through the API must name
an https issuer. They share the `localhost` certificate the suite generates for
the manager, and the manager trusts the CA behind it through `SSL_CERT_FILE`.
"""

from __future__ import annotations

import json
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
    cert: Path | None = None

    def token(
        self,
        subject: str,
        *,
        email: str | None = None,
        tenants: list[str] | None = None,
        audience: str | None = DEFAULT_AUDIENCE,
        expires_in: int = 3600,
        claims: dict | None = None,
        omit_claims: tuple[str, ...] = (),
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
        if claims is not None:
            params["claims"] = json.dumps(claims)
        if omit_claims:
            params["omit_claims"] = ",".join(omit_claims)
        r = requests.get(
            f"{self.url}/token",
            params=params,
            timeout=10,
            verify=str(self.cert) if self.cert else True,
        )
        r.raise_for_status()
        return r.json()["access_token"]

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()


def start_issuer(
    port: int,
    run_dir: Path,
    name: str = "idp",
    tls: tuple[Path, Path, Path] | None = None,
) -> Issuer:
    """Start `dummy_oidc.py` on `port` and wait for its discovery document.

    With `tls` (certificate, key, CA) the issuer serves https, which a trust
    registered through the API requires of it.
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / f"{name}.log"
    cert = tls[2] if tls else None
    url = f"{'https' if tls else 'http'}://localhost:{port}"
    log = log_path.open("ab")
    tls_args = ["--tls-cert", str(tls[0]), "--tls-key", str(tls[1])] if tls else []
    process = subprocess.Popen(
        ["uv", "run", str(DUMMY_OIDC), "--port", str(port), "--issuer", url, *tls_args],
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
            r = requests.get(
                f"{url}/.well-known/openid-configuration",
                timeout=2,
                verify=str(cert) if cert else True,
            )
            if r.status_code == 200:
                return Issuer(url=url, process=process, log_path=log_path, cert=cert)
        except requests.RequestException:
            pass
        time.sleep(0.5)
    process.kill()
    raise RuntimeError(
        f"{name} did not come up:\n{log_path.read_text(errors='replace')}"
    )
