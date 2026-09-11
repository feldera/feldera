"""Reading the expiry out of a bearer token without verifying it.

The instance verifies the signature. A client needs only to tell a token that
has run out of life apart from one the instance refuses for some other reason,
because that is what decides whether waiting for a background refresher could
help.
"""

from __future__ import annotations

import base64
import json
import time
from typing import Optional


def token_expiry(token: object) -> Optional[float]:
    """The `exp` claim of `token` as a Unix timestamp.

    None for anything that does not state one: an opaque API key, a malformed
    token, or a JWT whose payload carries no numeric `exp`.
    """
    if not isinstance(token, str):
        return None
    parts = token.split(".")
    if len(parts) != 3:
        return None
    # A JWT is unpadded base64url.
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except ValueError:
        return None
    if not isinstance(claims, dict):
        return None
    exp = claims.get("exp")
    # bool is an int, and `"exp": true` states no time.
    if isinstance(exp, bool) or not isinstance(exp, (int, float)):
        return None
    return float(exp)


def seconds_since_expiry(token: object, now: Optional[float] = None) -> Optional[float]:
    """How long ago `token` expired.

    None where it has not expired or does not say when it would, so a caller
    can treat "provably expired" as a distinct case from "rejected".
    """
    expiry = token_expiry(token)
    if expiry is None:
        return None
    elapsed = (time.time() if now is None else now) - expiry
    return elapsed if elapsed > 0 else None
