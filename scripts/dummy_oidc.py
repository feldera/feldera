# /// script
# requires-python = ">=3.10"
# dependencies = ["pyjwt", "cryptography"]
# ///
"""Dummy OIDC/OAuth2 identity provider for LOCAL TESTING of the Feldera
pipeline-manager with authentication enabled.

WARNING: DEV-ONLY, INSECURE. This issues signed JWTs for ANY user/role with no
authentication whatsoever. Never run this in or near production. Its sole
purpose is to exercise the manager's RBAC code paths by hand, in the web
console login flow, and in screenshots.

Two ways to obtain a token:

  1. Machine mint (scripts/rbac_demo.py path):
       GET /token?sub=&email=&tenants=a,b&groups=&aud=feldera-api&exp_secs=
       -> {"access_token", "token_type": "Bearer", "expires_in"}

  2. Browser login (web console, @axa-fr/oidc-client, code + PKCE):
       GET /authorize  -> role-picker page -> 302 back with ?code=&state=
       POST /token (grant_type=authorization_code) -> access_token + id_token
       GET /userinfo with the bearer access token.

The manager (crates/pipeline-manager/src/auth.rs) validates tokens by:
  1. fetching <issuer>/.well-known/openid-configuration to read jwks_uri,
  2. fetching the JWKS (RS256 keys: kid, kty=RSA, alg=RS256, use=sig, n, e),
  3. verifying an RS256 JWT whose header `kid` matches a JWKS key, and whose
     `iss` == issuer, `aud` == audience (default feldera-api), `exp` is valid.
"""

import argparse
import base64
import hashlib
import html
import json
import secrets
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlparse

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

KID = "dummy-key-1"
CODE_TTL_SECS = 600  # authorization codes live ~10 minutes

# Four demo identities offered on the login page. The effective role hint shows
# what each becomes once provisioned via scripts/rbac_demo.py.
ROLES: dict[str, dict] = {
    "reader": {
        "sub": "reader",
        "email": "reader@example.com",
        "tenants": ["acme"],
        "hint": "read access in tenant acme",
    },
    "writer": {
        "sub": "writer",
        "email": "writer@example.com",
        "tenants": ["acme"],
        "hint": "write access in tenant acme (after provisioning)",
    },
    "admin": {
        "sub": "admin",
        "email": "admin@example.com",
        "tenants": ["acme"],
        "hint": "admin of tenant acme",
    },
    "owner": {
        "sub": "owner",
        "email": "owner@example.com",
        # No tenants claim: owner selects a tenant via the Feldera-Tenant header.
        "hint": "platform owner (FELDERA_OWNERS)",
    },
}


def b64url(raw: bytes) -> str:
    """Base64url-encode without padding, as required by JWK n/e fields."""
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def int_to_b64url(value: int) -> str:
    """Encode a big integer as base64url big-endian bytes (JWK convention)."""
    length = (value.bit_length() + 7) // 8
    return b64url(value.to_bytes(length, "big"))


class KeyMaterial:
    """RSA-2048 keypair plus the JWK/PEM views the server needs."""

    def __init__(self) -> None:
        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.private_pem = self.private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        self.public_pem = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        numbers = self.private_key.public_key().public_numbers()
        self.jwk = {
            "kid": KID,
            "kty": "RSA",
            "alg": "RS256",
            "use": "sig",
            "n": int_to_b64url(numbers.n),
            "e": int_to_b64url(numbers.e),
        }


def make_handler(keys: KeyMaterial, issuer: str, default_audience: str):
    """Build a request handler bound to one keypair and issuer."""

    # In-memory, single-process stores. Codes are single-use; refresh tokens map
    # to the claims needed to re-mint a token pair.
    auth_codes: dict[str, dict] = {}
    refresh_tokens: dict[str, dict] = {}

    def discovery_doc() -> dict:
        return {
            "issuer": issuer,
            "authorization_endpoint": f"{issuer}/authorize",
            "token_endpoint": f"{issuer}/token",
            "userinfo_endpoint": f"{issuer}/userinfo",
            "jwks_uri": f"{issuer}/.well-known/jwks.json",
            "end_session_endpoint": f"{issuer}/logout",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256", "plain"],
            "scopes_supported": ["openid", "profile", "email", "offline_access"],
            "subject_types_supported": ["public"],
            "id_token_signing_alg_values_supported": ["RS256"],
            "token_endpoint_auth_methods_supported": [
                "none",
                "client_secret_post",
                "client_secret_basic",
            ],
            "claims_supported": [
                "sub",
                "email",
                "name",
                "preferred_username",
                "tenants",
                "groups",
            ],
        }

    def split_csv(value: str) -> list[str]:
        return [item.strip() for item in value.split(",") if item.strip()]

    def sign(claims: dict) -> str:
        return jwt.encode(
            claims, keys.private_pem, algorithm="RS256", headers={"kid": KID}
        )

    def mint_token(query: dict[str, list[str]]) -> dict:
        """Build claims from query params, then sign an RS256 JWT (machine mint)."""

        def one(name: str, default: str | None = None) -> str | None:
            values = query.get(name)
            return values[0] if values else default

        now = int(time.time())
        exp_secs = int(one("exp_secs", "3600"))
        audience = one("aud", default_audience)
        sub = one("sub", "dev-user")

        # Required + AWS-cognito-style claims. token_use=access mirrors real
        # access tokens; the manager treats it as optional for generic-oidc.
        claims: dict[str, object] = {
            "iss": issuer,
            "sub": sub,
            "aud": audience,
            "iat": now,
            "exp": now + exp_secs,
            "token_use": "access",
        }

        # Optional claims: include only when the caller supplies them, so tenant
        # resolution can fall back to `sub` when absent (individual_tenant=true).
        if (email := one("email")) is not None:
            claims["email"] = email
        if (tenant := one("tenant")) is not None:
            claims["tenant"] = tenant
        if (tenants := one("tenants")) is not None:
            claims["tenants"] = split_csv(tenants)
        if (groups := one("groups")) is not None:
            claims["groups"] = split_csv(groups)

        return {
            "access_token": sign(claims),
            "token_type": "Bearer",
            "expires_in": exp_secs,
        }

    def build_access_token(stored: dict) -> str:
        """Sign an access-token JWT from a stored authorize/refresh record."""
        now = int(time.time())
        claims: dict[str, object] = {
            "iss": issuer,
            "sub": stored["sub"],
            "aud": stored.get("audience") or default_audience,
            "iat": now,
            "exp": now + 3600,
            "token_use": "access",
            "email": stored["email"],
            "scope": stored.get("scope", "openid profile email"),
        }
        if stored.get("tenants"):
            claims["tenants"] = stored["tenants"]
        return sign(claims)

    def build_id_token(stored: dict) -> str:
        """Sign an id-token JWT from a stored authorize/refresh record."""
        now = int(time.time())
        sub = stored["sub"]
        claims: dict[str, object] = {
            "iss": issuer,
            "sub": sub,
            "aud": stored.get("client_id") or "feldera",
            "iat": now,
            "exp": now + 3600,
            "email": stored["email"],
            "name": sub.title(),
            "preferred_username": sub,
        }
        if stored.get("nonce"):
            claims["nonce"] = stored["nonce"]
        return sign(claims)

    def token_pair(stored: dict, refresh_token: str) -> dict:
        return {
            "access_token": build_access_token(stored),
            "id_token": build_id_token(stored),
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": refresh_token,
            "scope": stored.get("scope", "openid profile email"),
        }

    def verify_pkce(stored: dict, code_verifier: str | None) -> bool:
        challenge = stored.get("code_challenge")
        if not challenge:
            return True  # no challenge was sent at /authorize -> nothing to verify
        if not code_verifier:
            return False
        method = stored.get("code_challenge_method") or "plain"
        if method == "S256":
            digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
            return b64url(digest) == challenge
        return code_verifier == challenge  # plain

    def login_page_html(query: dict[str, list[str]]) -> str:
        """Render the role picker. Every button re-hits /authorize with the
        original params plus &login_as=<role>, preserving response_type, state,
        nonce, code_challenge, redirect_uri, audience, etc."""
        base_params = {k: v for k, v in query.items() if k != "login_as"}
        buttons = []
        for role, profile in ROLES.items():
            qs = urlencode({**base_params, "login_as": role}, doseq=True)
            hint = html.escape(profile["hint"])
            buttons.append(
                f'<a class="role" href="/authorize?{html.escape(qs)}">'
                f'<span class="role-name">{role}</span>'
                f'<span class="role-hint">{hint}</span></a>'
            )
        return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Feldera dev login</title>
<style>
  body {{ font-family: system-ui, sans-serif; background: #0f172a; color: #e2e8f0;
          margin: 0; min-height: 100vh; display: flex; align-items: center;
          justify-content: center; }}
  .card {{ background: #1e293b; padding: 2.5em; border-radius: 16px;
           box-shadow: 0 10px 40px rgba(0,0,0,.4); max-width: 28em; width: 90%; }}
  h1 {{ margin: 0 0 .2em; font-size: 1.6em; }}
  .sub {{ color: #94a3b8; margin: 0 0 1.5em; font-size: .9em; }}
  .grid {{ display: grid; gap: .75em; }}
  a.role {{ display: flex; flex-direction: column; text-decoration: none;
            background: #334155; color: #f1f5f9; padding: 1em 1.2em;
            border-radius: 10px; border: 1px solid #475569; transition: .15s; }}
  a.role:hover {{ background: #475569; border-color: #38bdf8; }}
  .role-name {{ font-size: 1.15em; font-weight: 600; text-transform: capitalize; }}
  .role-hint {{ color: #94a3b8; font-size: .85em; margin-top: .15em; }}
  .warn {{ margin-top: 1.5em; color: #fbbf24; font-size: .8em; text-align: center; }}
</style></head>
<body><div class="card">
  <h1>Feldera dev login</h1>
  <p class="sub">Pick a role to sign in as.</p>
  <div class="grid">{''.join(buttons)}</div>
  <p class="warn">DEV ONLY, INSECURE: no password, any role is granted on click.</p>
</div></body></html>"""

    def help_html() -> str:
        token_url = f"{issuer}/token?sub=alice&amp;email=alice@example.com"
        roles_rows = "".join(
            f"<tr><td><code>{r}</code></td><td><code>{p['email']}</code></td>"
            f"<td>{html.escape(p['hint'])}</td></tr>"
            for r, p in ROLES.items()
        )
        return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Dummy OIDC provider</title></head>
<body style="font-family: monospace; max-width: 55em; margin: 2em auto;">
<h1>Dummy OIDC provider (DEV ONLY, INSECURE)</h1>
<p>Issuer: <code>{issuer}</code></p>
<h2>Web console login</h2>
<p>The web console redirects here to <a href="/authorize">/authorize</a>,
   where you pick one of the roles below. DEV ONLY: any role is granted on click.</p>
<table border="1" cellpadding="6" cellspacing="0">
  <tr><th>role</th><th>email</th><th>effective role</th></tr>
  {roles_rows}
</table>
<h2>Endpoints</h2>
<ul>
  <li><a href="/.well-known/openid-configuration">/.well-known/openid-configuration</a></li>
  <li><a href="/.well-known/jwks.json">/.well-known/jwks.json</a></li>
  <li><a href="/authorize">/authorize</a> &mdash; browser login (code + PKCE)</li>
  <li><code>POST /token</code> &mdash; code / refresh-token exchange</li>
  <li><code>GET /userinfo</code> &mdash; profile from a bearer access token</li>
  <li><a href="/logout">/logout</a> &mdash; end session</li>
  <li><a href="/token?sub=alice&amp;email=alice@example.com">GET /token</a>
      &mdash; machine mint: sub, email, tenant, tenants, groups, aud, exp_secs (optional)</li>
</ul>
<h2>Mint a token and call the manager</h2>
<pre>TOKEN=$(curl -s '{token_url}' \\
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v0/pipelines</pre>
</body></html>"""

    def append_query(url: str, params: dict[str, str]) -> str:
        """Append params to a URL, respecting an existing query string."""
        sep = "&" if urlparse(url).query else "?"
        return f"{url}{sep}{urlencode(params)}"

    class Handler(BaseHTTPRequestHandler):
        def _cors(self) -> None:
            origin = self.headers.get("Origin") or "*"
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header(
                "Access-Control-Allow-Headers", "Authorization, Content-Type"
            )
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Vary", "Origin")

        def _send_json(self, payload: dict, status: int = 200) -> None:
            body = json.dumps(payload, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self._cors()
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, page: str, status: int = 200) -> None:
            body = page.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _redirect(self, location: str) -> None:
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

        # ---- /authorize: login page + code issuance --------------------------

        def _handle_authorize(self, query: dict[str, list[str]]) -> None:
            def one(name: str) -> str | None:
                values = query.get(name)
                return values[0] if values else None

            login_as = one("login_as")
            if login_as is None:
                self._send_html(login_page_html(query))
                return
            if login_as not in ROLES:
                self._send_json({"error": "invalid_request", "error_description": f"unknown role {login_as}"}, 400)
                return

            redirect_uri = one("redirect_uri")
            if not redirect_uri:
                self._send_json({"error": "invalid_request", "error_description": "missing redirect_uri"}, 400)
                return

            profile = ROLES[login_as]
            code = secrets.token_urlsafe(32)
            auth_codes[code] = {
                "sub": profile["sub"],
                "email": profile["email"],
                "tenants": profile.get("tenants"),
                "code_challenge": one("code_challenge"),
                "code_challenge_method": one("code_challenge_method"),
                "redirect_uri": redirect_uri,
                "nonce": one("nonce"),
                "scope": one("scope") or "openid profile email",
                "audience": one("audience") or default_audience,
                "client_id": one("client_id"),
                "expires_at": time.time() + CODE_TTL_SECS,
            }
            params = {"code": code}
            if (state := one("state")) is not None:
                params["state"] = state
            self._redirect(append_query(redirect_uri, params))

        # ---- POST /token: code / refresh exchange ----------------------------

        def _read_body_params(self) -> dict[str, str]:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            ctype = (self.headers.get("Content-Type") or "").split(";")[0].strip()
            if ctype == "application/json":
                try:
                    obj = json.loads(raw.decode("utf-8") or "{}")
                    return {k: str(v) for k, v in obj.items()}
                except (ValueError, TypeError):
                    return {}
            # default: application/x-www-form-urlencoded
            parsed = parse_qs(raw.decode("utf-8"))
            return {k: v[0] for k, v in parsed.items() if v}

        def _handle_token_post(self) -> None:
            params = self._read_body_params()
            grant_type = params.get("grant_type")

            if grant_type == "authorization_code":
                code = params.get("code")
                stored = auth_codes.pop(code, None) if code else None
                if not stored or stored["expires_at"] < time.time():
                    self._send_json({"error": "invalid_grant", "error_description": "code missing, expired, or already used"}, 400)
                    return
                if params.get("redirect_uri") != stored["redirect_uri"]:
                    self._send_json({"error": "invalid_grant", "error_description": "redirect_uri mismatch"}, 400)
                    return
                if not verify_pkce(stored, params.get("code_verifier")):
                    self._send_json({"error": "invalid_grant", "error_description": "PKCE verification failed"}, 400)
                    return
                refresh = secrets.token_urlsafe(32)
                refresh_tokens[refresh] = stored
                self._send_json(token_pair(stored, refresh))
                return

            if grant_type == "refresh_token":
                refresh = params.get("refresh_token")
                stored = refresh_tokens.get(refresh) if refresh else None
                if not stored:
                    self._send_json({"error": "invalid_grant", "error_description": "unknown refresh_token"}, 400)
                    return
                # Reuse the same refresh token (sufficient for dev).
                self._send_json(token_pair(stored, refresh))
                return

            self._send_json({"error": "unsupported_grant_type", "error_description": f"grant_type={grant_type}"}, 400)

        # ---- GET /userinfo ---------------------------------------------------

        def _handle_userinfo(self) -> None:
            auth = self.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                self._send_json({"error": "invalid_token", "error_description": "missing bearer token"}, 401)
                return
            token = auth[len("Bearer "):].strip()
            try:
                claims = jwt.decode(
                    token,
                    keys.public_pem,
                    algorithms=["RS256"],
                    options={"verify_aud": False},
                )
            except jwt.PyJWTError as err:
                self._send_json({"error": "invalid_token", "error_description": str(err)}, 401)
                return
            sub = claims.get("sub", "")
            info = {
                "sub": sub,
                "email": claims.get("email"),
                "name": sub.title(),
                "preferred_username": sub,
            }
            if claims.get("tenants"):
                info["tenants"] = claims["tenants"]
            self._send_json(info)

        # ---- /logout ---------------------------------------------------------

        def _handle_logout(self, query: dict[str, list[str]]) -> None:
            target = (query.get("post_logout_redirect_uri") or query.get("redirect_uri") or [None])[0]
            if target:
                self._redirect(target)
                return
            self._send_html(
                "<!doctype html><html><head><meta charset='utf-8'>"
                "<title>Logged out</title></head><body style='font-family:system-ui'>"
                "<h1>Logged out</h1><p>You may close this window.</p></body></html>"
            )

        # ---- dispatch --------------------------------------------------------

        def do_OPTIONS(self) -> None:  # noqa: N802 (http.server API)
            self.send_response(204)
            self._cors()
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self) -> None:  # noqa: N802 (http.server API)
            path = urlparse(self.path).path
            if path == "/token":
                self._handle_token_post()
            else:
                self._send_json({"error": "not found"}, status=404)

        def do_GET(self) -> None:  # noqa: N802 (http.server API)
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query)
            if path == "/.well-known/openid-configuration":
                self._send_json(discovery_doc())
            elif path == "/.well-known/jwks.json":
                self._send_json({"keys": [keys.jwk]})
            elif path == "/authorize":
                self._handle_authorize(query)
            elif path == "/token":
                # GET /token stays the machine-mint endpoint (rbac_demo.py).
                try:
                    self._send_json(mint_token(query))
                except (ValueError, TypeError) as err:
                    self._send_json({"error": f"bad request: {err}"}, status=400)
            elif path == "/userinfo":
                self._handle_userinfo()
            elif path == "/logout":
                self._handle_logout(query)
            elif path == "/":
                self._send_html(help_html())
            else:
                self._send_json({"error": "not found"}, status=404)

        def log_message(self, fmt: str, *args) -> None:
            # Keep one-line access logs; default impl is fine but quieter here.
            print(f"[dummy-oidc] {self.address_string()} {fmt % args}")

    return Handler


def print_startup(issuer: str, audience: str, port: int) -> None:
    print("=" * 70)
    print("Dummy OIDC provider running (DEV ONLY, INSECURE)")
    print(f"  Issuer:   {issuer}")
    print(f"  Audience: {audience}")
    print(f"  JWKS:     {issuer}/.well-known/jwks.json")
    print(f"  Login:    {issuer}/authorize  (web console redirects here)")
    print("-" * 70)
    print("Browser roles (pick one on the login page):")
    for role, profile in ROLES.items():
        print(f"  {role:7} {profile['email']:20} -> {profile['hint']}")
    print("-" * 70)
    print("Launch the pipeline-manager against it:")
    print(
        f"  AUTH_PROVIDER=generic-oidc FELDERA_AUTH_CLIENT_ID=feldera \\\n"
        f"    FELDERA_AUTH_ISSUER={issuer} FELDERA_AUTH_AUDIENCE={audience} \\\n"
        f"    FELDERA_OWNERS=owner@example.com \\\n"
        f"    cargo run --bin pipeline-manager"
    )
    print("-" * 70)
    print("Opening the web console redirects here to pick a role.")
    print("Or mint a token directly and call the manager:")
    print(
        f"  TOKEN=$(curl -s '{issuer}/token?sub=alice&email=alice@example.com' \\\n"
        f"    | python3 -c 'import sys,json;print(json.load(sys.stdin)[\"access_token\"])')"
    )
    print('  curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/v0/pipelines')
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dummy OIDC/OAuth2 provider for local Feldera auth testing (DEV ONLY)."
    )
    parser.add_argument("--port", type=int, default=9876, help="TCP port to listen on")
    parser.add_argument(
        "--issuer",
        default=None,
        help="Issuer URL; must match FELDERA_AUTH_ISSUER (default http://localhost:<port>)",
    )
    parser.add_argument(
        "--audience",
        default="feldera-api",
        help="Audience put in the aud claim; must match FELDERA_AUTH_AUDIENCE",
    )
    args = parser.parse_args()

    issuer = args.issuer or f"http://localhost:{args.port}"
    keys = KeyMaterial()
    handler = make_handler(keys, issuer, args.audience)
    server = ThreadingHTTPServer(("0.0.0.0", args.port), handler)

    print_startup(issuer, args.audience, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dummy OIDC provider.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
