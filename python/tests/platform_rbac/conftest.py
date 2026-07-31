"""Fixtures for the RBAC and OIDC-trust suite.

This suite owns the manager rather than attaching to a running one, because the
behaviour under test is what survives a restart under a different authentication
configuration. It therefore lives beside `tests/platform/` rather than inside
it: that package's conftest fetches an external OIDC token for the whole
session, which this suite must not inherit -- every identity here is minted
locally and deliberately.

The suite is stateful and ordered. Run it serially (`-n0`): scenarios hand
tenants and pipelines to one another, and tenant names are global to the
installation.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import pytest
import requests

from .idp import DEFAULT_AUDIENCE, Issuer, start_issuer
from .manager import AuthConfig, Manager, free_port, generate_tls_cert

TENANT_HEADER = "Feldera-Tenant"

# The tenant the scenarios build up, rename and finally delete.
TENANT = "acme"
OWNER_EMAIL = "owner@example.com"
# The workload identity the deployment trusts as a platform owner. It holds no
# login, which is what makes it a workload rather than a user.
OWNER_TRUST_SUBJECT = "ci-bot"
# The owner a later restart hands the installation to.
SUCCESSOR_EMAIL = "successor@example.com"


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers", "rbac: RBAC/OIDC suite; stateful and order-dependent, run serially"
    )


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo):
    """Attach the manager and issuer logs to a failing test.

    A failed assertion says what the API answered, not why. The manager fixture
    removes its container when it tears down, so CI's `always()` cleanup finds
    nothing left to dump; here the test's fixtures are still alive, which makes
    this the last moment the logs exist.
    """
    report = yield
    if report.when != "call" or not report.failed:
        return report
    funcargs = getattr(item, "funcargs", {})
    manager = funcargs.get("manager")
    if manager is not None:
        report.sections.append(("Manager log", manager.logs()[-8000:]))
    workdir = funcargs.get("workdir")
    if workdir is not None:
        for log in sorted(Path(workdir).glob("logs/*.log")):
            report.sections.append((log.stem, log.read_text(errors="replace")[-4000:]))
    return report


@pytest.fixture(scope="session")
def workdir() -> Path:
    """A short-pathed working directory.

    Not `tmp_path_factory`: the embedded Postgres puts its unix socket inside
    this tree, and the socket path is capped near 100 characters. pytest's
    temporary directories are already most of that budget on macOS, so the
    server fails to bind with a message that points nowhere near the cause.
    """
    base = Path(
        os.environ.get("FELDERA_RBAC_WORKDIR", f"/tmp/feldera-rbac-{os.getpid()}")
    )
    shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)
    yield base
    if not os.environ.get("FELDERA_RBAC_KEEP_WORKDIR"):
        shutil.rmtree(base, ignore_errors=True)


@pytest.fixture(scope="session")
def tls(workdir: Path) -> tuple[Path, Path, Path]:
    """The suite's CA and the `localhost` certificate it signs.

    A trust registered through the API must name an https issuer, so every
    issuer here serves TLS, and the manager is pointed at the CA through
    `SSL_CERT_FILE`.
    """
    return generate_tls_cert(workdir / "run" / "tls")


@pytest.fixture(scope="session")
def primary_idp(workdir: Path, tls: tuple[Path, Path, Path]) -> Issuer:
    """The issuer every authenticated configuration trusts."""
    issuer = start_issuer(free_port(), workdir / "logs", name="primary-idp", tls=tls)
    yield issuer
    issuer.stop()


@pytest.fixture(scope="session")
def workload_idp(workdir: Path, tls: tuple[Path, Path, Path]) -> Issuer:
    """A trusted issuer that is not the login provider.

    Trusts are registered against this one through the API.
    """
    issuer = start_issuer(free_port(), workdir / "logs", name="workload-idp", tls=tls)
    yield issuer
    issuer.stop()


@pytest.fixture(scope="session")
def rogue_idp(workdir: Path, tls: tuple[Path, Path, Path]) -> Issuer:
    """A second issuer the manager trusts for nothing.

    It signs with its own key, so a token from it fails verification even when
    it claims the trusted issuer's `iss`.
    """
    issuer = start_issuer(free_port(), workdir / "logs", name="rogue-idp", tls=tls)
    yield issuer
    issuer.stop()


@pytest.fixture(scope="session")
def manager(workdir: Path, tls: tuple[Path, Path, Path]) -> Manager:
    """The manager under test, shared and restarted by the scenarios."""
    mgr = Manager(state_dir=workdir / "state", run_dir=workdir / "run", https=True)
    yield mgr
    mgr.stop()
    mgr.remove_volume()


# Authentication configurations the scenarios boot under
def no_auth() -> AuthConfig:
    return AuthConfig(name="no-auth", env={"AUTH_PROVIDER": "none"})


def single_tenant_auth(
    idp: Issuer,
    workload_idp: Issuer | None = None,
    owners: str = OWNER_EMAIL,
) -> AuthConfig:
    """Authenticated against `idp`, identities keyed by subject.

    Passing `workload_idp` also configures a platform-wide owner trust for a
    workload on that issuer.
    """
    env = {
        "AUTH_PROVIDER": "generic-oidc",
        "FELDERA_AUTH_CLIENT_ID": "feldera",
        # The suite's issuers run on localhost, so a trust registered against
        # one names a loopback address. That is the installation this flag
        # exists for, and without it the manager refuses to fetch their keys.
        "FELDERA_ALLOW_INTERNAL_TENANT_TRUST_ISSUERS": "true",
        "FELDERA_AUTH_ISSUER": idp.url,
        "FELDERA_AUTH_AUDIENCE": DEFAULT_AUDIENCE,
        "FELDERA_OWNERS": owners,
    }
    if workload_idp is not None:
        env["FELDERA_OWNER_TRUSTS"] = json.dumps(
            [
                {
                    "issuer": workload_idp.url,
                    "subject": OWNER_TRUST_SUBJECT,
                    "audience": DEFAULT_AUDIENCE,
                }
            ]
        )
    return AuthConfig(
        name="single-tenant" + ("-ownertrust" if workload_idp else ""), env=env
    )


def multi_tenant_auth(
    idp: Issuer,
    workload_idp: Issuer | None = None,
    owners: str = OWNER_EMAIL,
) -> AuthConfig:
    """Authenticated, and a token's `tenants` claim may name several tenants.

    The same subject can then hold a different role in each, which is the case
    the single-tenant configuration cannot express.
    """
    config = single_tenant_auth(idp, workload_idp, owners)
    return AuthConfig(name="multi-tenant", env=config.env)


# Talking to the manager
class Api:
    """Raw REST against the manager.

    Deliberately not the SDK: these tests assert on exact status codes, send
    tokens the SDK would refuse to construct, and must never retry or refresh a
    credential behind the assertion's back.
    """

    def __init__(self, manager: Manager):
        self.manager = manager

    def request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        tenant: str | None = None,
        body: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        hdrs = dict(headers or {})
        if token is not None:
            hdrs["Authorization"] = f"Bearer {token}"
        if tenant is not None:
            hdrs[TENANT_HEADER] = tenant
        return requests.request(
            method,
            f"{self.manager.base_url}{path}",
            headers=hdrs,
            json=body,
            timeout=30,
            verify=self.manager.verify,
        )

    def v0(self, method: str, path: str, **kwargs) -> requests.Response:
        return self.request(method, f"/v0{path}", **kwargs)

    def status(self, method: str, path: str, **kwargs) -> int:
        return self.v0(method, path, **kwargs).status_code


@pytest.fixture(scope="session")
def api(manager: Manager) -> Api:
    return Api(manager)
