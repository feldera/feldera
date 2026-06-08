#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["requests"]
# ///
"""
RBAC demo provisioner: set up one user per role and prove the boundaries.

DEV ONLY. Assumes two things are already running:

  1. The dummy OIDC issuer:   uv run scripts/dummy_oidc.py            (:9876)
  2. The pipeline-manager with auth enabled AND owner@example.com listed
     as a platform owner, for example:

       AUTH_PROVIDER=generic-oidc FELDERA_AUTH_CLIENT_ID=feldera \\
       FELDERA_AUTH_ISSUER=http://localhost:9876 FELDERA_AUTH_AUDIENCE=feldera-api \\
       FELDERA_OWNERS=owner@example.com \\
       FELDERA_UNSTABLE_FEATURES='runtime_version,testing' \\
       cargo run --release --bin=pipeline-manager --features feldera-enterprise \\
         -- --runner-working-directory=/mnt/data/feldera

This script then:
  - mints tokens for four identities,
  - provisions them into one shared tenant ("acme") with roles read/write/admin,
  - leaves owner@example.com as the platform owner (from FELDERA_OWNERS),
  - prints a ready-to-use token per role, and
  - runs a request matrix showing what each role may and may not do.

The four identities (all via the dummy IdP):

  | role  | email             | how the role is granted                         |
  |-------|-------------------|-------------------------------------------------|
  | owner | owner@example.com | FELDERA_OWNERS (config); acts in any tenant     |
  | admin | admin@example.com | owner assigns 'admin' in tenant acme            |
  | write | writer@example.com| owner assigns 'write' in tenant acme            |
  | read  | reader@example.com| default role on first login to acme             |
"""

import argparse
import sys
import requests

TENANT_HEADER = "Feldera-Tenant"


def mint(oidc: str, sub: str, email: str, tenants: list[str] | None) -> str:
    params = {"sub": sub, "email": email, "aud": "feldera-api"}
    if tenants:
        params["tenants"] = ",".join(tenants)
    r = requests.get(f"{oidc}/token", params=params, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


def call(manager, method, path, token, tenant=None, body=None):
    headers = {"Authorization": f"Bearer {token}"}
    if tenant:
        headers[TENANT_HEADER] = tenant
    resp = requests.request(
        method, f"{manager}/v0{path}", headers=headers, json=body, timeout=15
    )
    return resp.status_code, resp


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--manager", default="http://localhost:8080")
    ap.add_argument("--oidc", default="http://localhost:9876")
    ap.add_argument("--tenant", default="acme")
    args = ap.parse_args()
    m, tenant = args.manager.rstrip("/"), args.tenant

    # 1. Mint tokens. read/write/admin share tenant `acme` via the tenants claim;
    #    owner needs no tenant claim (it selects a tenant with the header).
    print(f"Minting tokens from {args.oidc} ...")
    tok = {
        "owner": mint(args.oidc, "owner", "owner@example.com", None),
        "admin": mint(args.oidc, "admin", "admin@example.com", [tenant]),
        "write": mint(args.oidc, "writer", "writer@example.com", [tenant]),
        "read": mint(args.oidc, "reader", "reader@example.com", [tenant]),
    }

    # 2. admin@ logs in FIRST. Because the tenant does not exist yet, the
    #    creator becomes its admin. Letting the login create the tenant keeps a
    #    single (name, provider=issuer) row; pre-creating it as an owner would
    #    use a different provider and collide on the name.
    code, _ = call(m, "GET", "/config/session", tok["admin"], tenant=tenant)
    print(f"  admin first login (creates '{tenant}', becomes admin): HTTP {code}")
    if code != 200:
        print(
            "  !! admin login failed. Is the manager running with auth (dummy OIDC)? Aborting.",
            file=sys.stderr,
        )
        return 1

    # 3. writer/reader log in -> default role (read).
    for role in ("write", "read"):
        code, _ = call(m, "GET", "/config/session", tok[role], tenant=tenant)
        print(f"  {role} first login -> membership (default read): HTTP {code}")

    # 4. The admin promotes writer -> write (admin manages its own tenant).
    code, resp = call(m, "GET", "/tenant/users", tok["admin"], tenant=tenant)
    members = {u["email"]: u["user_id"] for u in resp.json()} if code == 200 else {}
    wid = members.get("writer@example.com")
    if wid:
        code, _ = call(
            m, "PUT", f"/tenant/users/{wid}", tok["admin"], tenant=tenant,
            body={"role": "write"},
        )
        print(f"  admin set writer@ -> write: HTTP {code}")
    else:
        print("  !! writer@ not found among members; the write row may fail", file=sys.stderr)

    # 5. Print ready-to-use tokens.
    print("\n" + "=" * 72)
    print("READY: one user per role. Export a token and call the API, e.g.")
    print(f'  curl -H "Authorization: Bearer $READ"  {m}/v0/pipelines')
    print("=" * 72)
    for role in ("read", "write", "admin", "owner"):
        print(f"\n{role.upper()}={tok[role]}")
    if True:
        print(f"\n(owner acts in a tenant by adding the header: -H '{TENANT_HEADER}: {tenant}')")

    # 6. Verification matrix: each row is (role, method, path, expected, note).
    #    `expected` is the status family we assert: 'ok' = not 403, 'deny' = 403.
    print("\n" + "=" * 72)
    print("VERIFICATION MATRIX (deny = 403 by RBAC; ok = passed RBAC)")
    print("=" * 72)
    checks = [
        ("read",  "GET",  "/pipelines",     None,                  "ok",   "monitor"),
        ("read",  "POST", "/pipelines",     {"name": "x"},         "deny", "no mutate"),
        ("read",  "GET",  "/tenant/users",  None,                  "deny", "not admin"),
        ("write", "GET",  "/pipelines",     None,                  "ok",   "monitor"),
        ("write", "POST", "/api_keys",      {"name": "w1", "role": "read"},  "ok",   "mint read key"),
        ("write", "POST", "/api_keys",      {"name": "w2", "role": "admin"}, "deny", "cannot mint admin key"),
        ("write", "GET",  "/tenant/users",  None,                  "deny", "not admin"),
        ("admin", "GET",  "/tenant/users",  None,                  "ok",   "manage users"),
        ("admin", "POST", "/oidc_trust",    {"name": "t1", "issuer": "https://x", "subject": "s", "role": "write"}, "ok", "create trust"),
        ("admin", "GET",  "/tenants",       None,                  "deny", "owner only"),
        ("owner", "GET",  "/tenants",       None,                  "ok",   "platform view"),
        ("owner", "GET",  "/tenant/users",  None,                  "ok",   "acts in tenant"),
    ]
    passed = failed = 0
    for role, method, path, body, expected, note in checks:
        tn = tenant if role in ("read", "write", "admin", "owner") else None
        code, _ = call(m, method, path, tok[role], tenant=tn, body=body)
        # ok = passed RBAC and the action succeeded (2xx); deny = blocked by RBAC (403).
        # Anything else (401, 5xx) is an unexpected failure, not a pass.
        good = (expected == "deny" and code == 403) or (
            expected == "ok" and 200 <= code < 300
        )
        passed += good
        failed += not good
        mark = "PASS" if good else "FAIL"
        print(f"  [{mark}] {role:5} {method:4} {path:16} -> {code:3} (want {expected:4}; {note})")
    print(f"\n{passed} passed, {failed} failed")
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
