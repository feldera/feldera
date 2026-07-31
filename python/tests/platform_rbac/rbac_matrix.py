"""Every gated route, and the role it demands, read from the OpenAPI spec.

The manager annotates each operation with "Required role: `x`" while building
its spec, from the same table the middleware enforces.

Path parameters name resources that do not exist and mutating methods carry a
body the manager cannot deserialize, so a caller that clears RBAC lands on 400
or 404 instead of changing anything.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
OPENAPI_PATH = REPO_ROOT / "openapi.json"

ROLE_ORDER = ["read", "write", "admin", "owner"]

# A name no fixture creates, so every probe misses.
ABSENT = "rbac-probe-does-not-exist"
ABSENT_UUID = "00000000-0000-4000-8000-000000000000"

# `GET /config/authentication` advertises how to authenticate, so it carries no
# role and must stay reachable without a token.
UNGATED = {("GET", "/config/authentication")}


@dataclass(frozen=True)
class Route:
    method: str
    path: str  # OpenAPI template, e.g. /v0/pipelines/{pipeline_name}
    required_role: str

    @property
    def probe_path(self) -> str:
        """`path` with every parameter replaced by something absent."""

        def substitute(match: re.Match) -> str:
            name = match.group(1)
            return ABSENT_UUID if name.endswith("_id") else ABSENT

        return re.sub(r"\{(\w+)\}", substitute, self.path)

    @property
    def id(self) -> str:
        return f"{self.method} {self.path}"

    def allows(self, role: str) -> bool:
        return ROLE_ORDER.index(role) >= ROLE_ORDER.index(self.required_role)


def load_routes(spec_path: Path = OPENAPI_PATH) -> list[Route]:
    """Every gated operation in the spec, sorted for a stable test order."""
    spec = json.loads(spec_path.read_text())
    routes: list[Route] = []
    for path, operations in spec["paths"].items():
        for method, operation in operations.items():
            if method.upper() not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
                continue
            verb = method.upper()
            if (verb, path) in UNGATED:
                continue
            match = re.search(
                r"Required role: `(read|write|admin|owner)`",
                operation.get("description") or "",
            )
            if not match:
                raise AssertionError(
                    f"{verb} {path} declares no required role. Either add it to "
                    f"the RBAC table so the annotation is generated, or list it "
                    f"in UNGATED with a reason."
                )
            routes.append(Route(verb, path, match.group(1)))
    routes.sort(key=lambda r: (r.path, r.method))
    return routes


def probe_body(route: Route) -> dict | None:
    """Inject a dummy payload."""
    if route.method in {"POST", "PUT", "PATCH"}:
        return {"__rbac_probe__": True}
    return None
