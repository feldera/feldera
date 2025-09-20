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
        Obtain access token using global cache shared across all helper instances.
        
        The actual token fetching is handled by the session-scoped oidc_token_fixture
        in conftest.py, which guarantees only one auth request per test session.
        
        If OIDC is configured but no token is available, this will fail fast.
        """
        global _global_token_cache
        logger = logging.getLogger(__name__)

        # First check environment variable for cross-process token sharing
        current_time = time.time()
        env_token = os.getenv('FELDERA_PYTEST_OIDC_TOKEN')
        if env_token:
            try:
                import base64
                token_json = base64.b64decode(env_token.encode()).decode()
                token_data = json.loads(token_json)
                
                if token_data.get('access_token') and current_time < token_data.get('expires_at', 0) - 30:
                    print("🔐 AUTH: Using token from environment variable")
                    logger.info("Using environment variable cached access token")
                    # Cache in global and instance for future calls
                    _global_token_cache['access_token'] = token_data['access_token']
                    _global_token_cache['expires_at'] = token_data['expires_at']
                    self._access_token = token_data['access_token']
                    self._token_expires_at = token_data['expires_at']
                    return token_data['access_token']
            except Exception as e:
                print(f"🔐 AUTH: Failed to parse environment token: {e}")

        # Second check global cache (shared across all instances)
        if _global_token_cache['access_token'] and current_time < _global_token_cache['expires_at'] - 30:
            logger.info("Using global cached access token")
            logger.debug(f"Global cached token (first 20 chars): {_global_token_cache['access_token'][:20]}...")
            return _global_token_cache['access_token']

        # Fallback: Check instance-specific cache
        if self._access_token and current_time < self._token_expires_at - 30:
            logger.info("Using instance cached access token")
            logger.debug(f"Instance cached token (first 20 chars): {self._access_token[:20]}...")
            # Copy to global cache for other instances
            _global_token_cache['access_token'] = self._access_token
            _global_token_cache['expires_at'] = self._token_expires_at
            return self._access_token

        # Debug information for troubleshooting
        print(f"🔐 AUTH: DEBUG - Token check failed for instance {id(self)}")
        print(f"🔐 AUTH: DEBUG - Global cache ID: {id(_global_token_cache)}")
        print(f"🔐 AUTH: DEBUG - Global cache token: {_global_token_cache['access_token'] is not None}")
        print(f"🔐 AUTH: DEBUG - Global cache expires_at: {_global_token_cache['expires_at']}")
        print(f"🔐 AUTH: DEBUG - Instance _access_token: {self._access_token is not None}")
        print(f"🔐 AUTH: DEBUG - Instance _token_expires_at: {self._token_expires_at}")
        print(f"🔐 AUTH: DEBUG - Current time: {current_time}")

        # If OIDC is configured but no token is available, this is a critical failure
        raise RuntimeError(
            f"OIDC authentication is configured but no valid token is available. "
            f"This indicates the oidc_token_fixture failed to retrieve a token. "
            f"Helper instance ID: {id(self)}, global_cache_token: {_global_token_cache['access_token'] is not None}, "
            f"instance_token: {self._access_token is not None}, current_time: {current_time}"
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

# Global token cache shared across all helper instances
_global_token_cache = {
    'access_token': None,
    'expires_at': 0
}


def get_oidc_test_helper() -> Optional[OidcTestHelper]:
    """Get global OIDC test helper instance"""
    global _test_helper
    if _test_helper is None:
        config = OidcTestConfig.from_environment()
        if config:
            _test_helper = OidcTestHelper(config)
    return _test_helper