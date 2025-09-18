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
        token_data = get_cached_token_from_env()
        if token_data:
            logger.info("Using environment variable cached access token")
            # Cache in instance for future calls to avoid repeated parsing
            self._access_token = token_data["access_token"]
            self._token_expires_at = token_data["expires_at"]
            return token_data["access_token"]

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


def parse_cached_token(env_token: str) -> Optional[Dict[str, Any]]:
    """
    Parse and validate a base64-encoded token from environment variable.

    Args:
        env_token: Base64-encoded JSON token data from environment variable

    Returns:
        Parsed token data dict if valid, None if invalid or expired
    """
    try:
        import base64

        token_json = base64.b64decode(env_token.encode()).decode()
        token_data = json.loads(token_json)
        return token_data
    except Exception as e:
        logging.getLogger(__name__).warning(f"Failed to parse cached token: {e}")
        return None


def is_token_valid(token_data: Dict[str, Any], buffer_seconds: int = 30) -> bool:
    """
    Check if token data is valid and not expired.

    Args:
        token_data: Dictionary containing token information
        buffer_seconds: Safety buffer before expiration (default 30 seconds)

    Returns:
        True if token is valid and not expired, False otherwise
    """
    if not token_data:
        return False

    access_token = token_data.get("access_token")
    expires_at = token_data.get("expires_at", 0)

    if not access_token:
        return False

    current_time = time.time()
    return current_time < expires_at - buffer_seconds


def encode_token_for_env(token_data: Dict[str, Any]) -> str:
    """
    Encode token data as base64 for storage in environment variables.

    Args:
        token_data: Dictionary containing token information

    Returns:
        Base64-encoded JSON string suitable for environment variable storage
    """
    import base64

    token_json = json.dumps(token_data)
    return base64.b64encode(token_json.encode()).decode()


def get_cached_token_from_env(
    env_var_name: str = "FELDERA_PYTEST_OIDC_TOKEN",
) -> Optional[Dict[str, Any]]:
    """
    Retrieve and validate cached token from environment variable.

    Args:
        env_var_name: Name of environment variable containing cached token

    Returns:
        Valid token data if available and not expired, None otherwise
    """
    import os

    env_token = os.getenv(env_var_name)
    if not env_token:
        return None

    token_data = parse_cached_token(env_token)
    if token_data and is_token_valid(token_data):
        return token_data

    return None


def setup_token_cache() -> Optional[Dict[str, Any]]:
    """
    Set up OIDC token cache in environment variable if not already present.

    This function:
    1. Checks if a valid token is already cached
    2. If not, fetches a new token
    3. Stores the token in environment variable for cross-process access

    Used by both pytest hooks and demo runners to ensure consistent token caching.

    Returns:
        Token data if successfully cached, None if OIDC not configured
    """
    import os

    # Check if token is already cached and still valid
    cached_token = get_cached_token_from_env()
    if cached_token:
        print("🔐 AUTH: Using existing cached OIDC token")
        return cached_token

    # Fetch new token if needed
    token_data = fetch_oidc_token()
    if token_data:
        # Store in environment variable for reuse by subsequent processes
        token_b64 = encode_token_for_env(token_data)
        os.environ["FELDERA_PYTEST_OIDC_TOKEN"] = token_b64
        print("🔐 AUTH: Token cached in environment for cross-process access")
        return token_data
    else:
        print("🔐 AUTH: No OIDC configuration found, using fallback authentication")
        return None


def fetch_oidc_token() -> Optional[Dict[str, Any]]:
    """
    Fetch OIDC token using Resource Owner Password Grant flow.

    This function is used by both pytest hooks and demo runners to ensure
    consistent token fetching behavior across the entire test infrastructure.

    Returns:
        Dict containing access_token, expires_at, and cached_at if successful,
        None if OIDC is not configured.
    """
    oidc_helper = get_oidc_test_helper()
    if oidc_helper is None:
        return None

    print("🔐 AUTH: Fetching OIDC token")

    try:
        token_endpoint = oidc_helper.get_token_endpoint()
        data = {
            "grant_type": "password",
            "username": oidc_helper.config.username,
            "password": oidc_helper.config.password,
            "client_id": oidc_helper.config.client_id,
            "client_secret": oidc_helper.config.client_secret,
            "scope": oidc_helper.config.scope,
            "audience": "feldera-api",
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        response = requests.post(token_endpoint, data=data, headers=headers, timeout=30)

        if not response.ok:
            print(f"🔐 AUTH: ❌ Token request FAILED: {response.status_code}")
            raise Exception(
                f"Token request failed: {response.status_code} - {response.text}"
            )

        token_response = response.json()
        print("🔐 AUTH: ✅ Token request SUCCESS!")

        access_token = token_response["access_token"]
        expires_in = token_response.get("expires_in", 3600)
        expires_at = time.time() + expires_in

        return {
            "access_token": access_token,
            "expires_at": expires_at,
            "cached_at": time.time(),
        }

    except Exception as e:
        print(f"🔐 AUTH: CRITICAL FAILURE - Failed to fetch OIDC token: {e}")
        raise RuntimeError(
            f"OIDC authentication is configured but token retrieval failed: {e}"
        ) from e
