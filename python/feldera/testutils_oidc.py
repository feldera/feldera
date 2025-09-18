"""
OIDC Authentication Test Helper

Utilities for testing OIDC authentication integration with remote providers.
Provides token generation, validation helpers, and test configuration.
"""

import os
import time
import json
import requests
import jwt
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class OidcTestConfig:
    """Configuration for OIDC authentication tests using Resource Owner Password Flow"""

    issuer: str
    client_id: str
    client_secret: str
    username: str
    password: str
    scope: str = "openid profile email"

    @classmethod
    def from_environment(cls) -> Optional["OidcTestConfig"]:
        """Load OIDC test configuration from environment variables"""
        issuer = os.getenv("OIDC_TEST_ISSUER")
        client_id = os.getenv("OIDC_TEST_CLIENT_ID")
        client_secret = os.getenv("OIDC_TEST_CLIENT_SECRET")
        username = os.getenv("OIDC_TEST_USERNAME")
        password = os.getenv("OIDC_TEST_PASSWORD")

        # All fields are required
        if not all([issuer, client_id, client_secret, username, password]):
            return None

        return cls(
            issuer=issuer,
            client_id=client_id,
            client_secret=client_secret,
            username=username,
            password=password,
            scope=os.getenv("OIDC_TEST_SCOPE", "openid profile email"),
        )


class OidcTestHelper:
    """Helper class for OIDC authentication testing"""

    def __init__(self, config: OidcTestConfig):
        self.config = config
        self._discovery_doc = None
        self._jwks = None
        self._access_token = None
        self._token_expires_at = 0

    def get_discovery_document(self) -> Dict[str, Any]:
        """Fetch and cache the OIDC discovery document"""
        if self._discovery_doc is None:
            discovery_url = (
                f"{self.config.issuer.rstrip('/')}/.well-known/openid-configuration"
            )
            response = requests.get(discovery_url, timeout=30)
            response.raise_for_status()
            self._discovery_doc = response.json()
        return self._discovery_doc

    def get_jwks(self) -> Dict[str, Any]:
        """Fetch and cache the JSON Web Key Set"""
        if self._jwks is None:
            discovery_doc = self.get_discovery_document()
            jwks_uri = discovery_doc["jwks_uri"]
            response = requests.get(jwks_uri, timeout=30)
            response.raise_for_status()
            self._jwks = response.json()
        return self._jwks

    def get_token_endpoint(self) -> str:
        """Get the token endpoint URL from discovery document"""
        discovery_doc = self.get_discovery_document()
        token_endpoint = discovery_doc.get("token_endpoint")
        if not token_endpoint:
            raise ValueError("Token endpoint not found in OIDC discovery document")
        return token_endpoint

    def obtain_access_token(self, pytest_cache=None) -> str:
        """
        Obtain access token using environment variable set by pytest master node.

        The actual token fetching is handled by pytest_configure hooks in conftest.py,
        which guarantees only one auth request per test session across all workers.

        If OIDC is configured but no token is available, this will fail fast.
        """
        logger = logging.getLogger(__name__)
        current_time = time.time()

        # Check environment variable for cross-process token sharing
        env_token = os.getenv("FELDERA_PYTEST_OIDC_TOKEN")
        if env_token:
            try:
                import base64

                token_json = base64.b64decode(env_token.encode()).decode()
                token_data = json.loads(token_json)

                if (
                    token_data.get("access_token")
                    and current_time < token_data.get("expires_at", 0) - 30
                ):
                    logger.info("Using environment variable cached access token")
                    # Cache in instance for future calls to avoid repeated parsing
                    self._access_token = token_data["access_token"]
                    self._token_expires_at = token_data["expires_at"]
                    return token_data["access_token"]
            except Exception as e:
                logger.warning(f"Failed to parse environment token: {e}")

        # Fallback: Check instance cache
        if self._access_token and current_time < self._token_expires_at - 30:
            logger.info("Using instance cached access token")
            return self._access_token

        # If OIDC is configured but no token is available, this is a critical failure
        raise RuntimeError(
            "OIDC authentication is configured but no valid token is available. "
            "This indicates the oidc_token_fixture failed to retrieve a token. "
            "Check OIDC configuration and ensure pytest hooks ran properly."
        )

    def decode_token_claims(self, token: str) -> Dict[str, Any]:
        """Decode JWT token claims without signature verification (for testing)"""
        return jwt.decode(token, options={"verify_signature": False})

    def is_token_expired(self, token: str) -> bool:
        """Check if a JWT token is expired"""
        try:
            claims = self.decode_token_claims(token)
            exp = claims.get("exp")
            if exp is None:
                return False  # No expiration claim
            return time.time() > exp
        except Exception:
            return True  # Invalid token is considered expired

    def validate_token_structure(self, token: str) -> bool:
        """Validate that token has correct JWT structure"""
        try:
            # Just check if it can be decoded (ignoring signature)
            self.decode_token_claims(token)
            return True
        except Exception:
            return False

    def create_authenticated_headers(self) -> Dict[str, str]:
        """Create HTTP headers with valid authentication token"""
        token = self.obtain_access_token()
        return {"Accept": "application/json", "Authorization": f"Bearer {token}"}

    def get_malformed_test_tokens(self) -> Dict[str, str]:
        """Get various malformed tokens for negative testing"""
        return {
            "malformed_structure": "not.a.jwt",
            "empty": "",
            "malformed_header": "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.invalid",
            "wrong_issuer": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJodHRwczovL3dyb25nLWlzc3Vlci5jb20iLCJhdWQiOiJ0ZXN0IiwiZXhwIjo5OTk5OTk5OTk5fQ.signature",
        }


def skip_if_oidc_not_configured():
    """Decorator to skip tests if OIDC test environment is not configured"""
    import pytest

    config = OidcTestConfig.from_environment()
    return pytest.mark.skipif(
        config is None,
        reason="OIDC test environment not configured. Set OIDC_TEST_ISSUER, OIDC_TEST_CLIENT_ID, OIDC_TEST_CLIENT_SECRET, OIDC_TEST_USERNAME, OIDC_TEST_PASSWORD",
    )


# Global test helper instance (lazy loaded)
_test_helper = None


def get_oidc_test_helper() -> Optional[OidcTestHelper]:
    """Get global OIDC test helper instance"""
    global _test_helper
    if _test_helper is None:
        config = OidcTestConfig.from_environment()
        if config:
            _test_helper = OidcTestHelper(config)
    return _test_helper
