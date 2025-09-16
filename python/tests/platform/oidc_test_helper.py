"""
OIDC Authentication Test Helper

Utilities for testing OIDC authentication integration with remote providers.
Provides token generation, validation helpers, and test configuration.
"""

import os
import time
import requests
import jwt
import logging
import json
import tempfile
import fcntl
from pathlib import Path
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
        # Cross-process token cache file path
        self._token_cache_file = Path(tempfile.gettempdir()) / f"feldera_oidc_token_{hash(self.config.issuer + self.config.client_id + self.config.username)}.json"

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

    def _load_cached_token(self) -> Optional[str]:
        """Load token from cross-process cache if still valid"""
        try:
            if not self._token_cache_file.exists():
                return None

            with open(self._token_cache_file, 'r') as f:
                # Use file locking for thread/process safety
                fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                cache_data = json.load(f)

            token = cache_data.get('access_token')
            expires_at = cache_data.get('expires_at', 0)

            # Check if token is still valid (with 30s buffer)
            if token and time.time() < expires_at - 30:
                return token

        except (FileNotFoundError, json.JSONDecodeError, KeyError, OSError):
            # If any error reading cache, just return None to fetch new token
            pass

        return None

    def _save_token_to_file_cache(self, token: str, expires_in: int):
        """Save token to cross-process cache"""
        try:
            cache_data = {
                'access_token': token,
                'expires_at': time.time() + expires_in,
                'cached_at': time.time()
            }

            # Create directory if it doesn't exist
            self._token_cache_file.parent.mkdir(exist_ok=True)

            # Use atomic write with file locking
            temp_file = self._token_cache_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                json.dump(cache_data, f)

            # Atomic move
            temp_file.rename(self._token_cache_file)

        except (OSError, IOError) as e:
            # Log warning but don't fail - we can still use the token without caching
            logging.warning(f"Failed to save OIDC token to cache: {e}")

    def obtain_access_token(self) -> str:
        """
        Obtain access token using Resource Owner Password Flow
        Returns cached token if still valid (checks both in-memory and cross-process cache)
        """
        logger = logging.getLogger(__name__)

        # First check in-memory cache
        if self._access_token and time.time() < self._token_expires_at - 30:
            logger.info("Using in-memory cached access token")
            logger.debug(f"Cached token (first 20 chars): {self._access_token[:20]}...")
            return self._access_token

        # Then check cross-process cache
        cached_token = self._load_cached_token()
        if cached_token:
            logger.info("Using cross-process cached access token")
            logger.debug(f"Cross-process cached token (first 20 chars): {cached_token[:20]}...")
            # Update in-memory cache with the token from cross-process cache
            self._access_token = cached_token
            return cached_token

        token_endpoint = self.get_token_endpoint()
        logger.info(f"Requesting new access token from: {token_endpoint}")

        data = {
            "grant_type": "password",
            "username": self.config.username,
            "password": self.config.password,
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "scope": self.config.scope,
            "audience": "feldera-api",
        }

        # Log request details (without sensitive data)
        logger.info(f"Token request - username: {self.config.username}")
        logger.info(f"Token request - client_id: {self.config.client_id}")
        logger.info(f"Token request - scope: {self.config.scope}")
        logger.info("Token request - audience: feldera-api")

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        response = requests.post(token_endpoint, data=data, headers=headers, timeout=30)

        if not response.ok:
            logger.error(f"Token request failed: {response.status_code}")
            logger.error(f"Response headers: {dict(response.headers)}")
            logger.error(f"Response body: {response.text}")
            raise Exception(
                f"Token request failed: {response.status_code} - {response.text}"
            )

        token_response = response.json()
        logger.info("Successfully obtained access token")

        # Cache the token both in-memory and cross-process
        self._access_token = token_response["access_token"]
        expires_in = token_response.get("expires_in", 3600)  # Default to 1 hour if not provided
        self._token_expires_at = time.time() + expires_in
        logger.info(f"Token expires in {expires_in} seconds")

        # Save to cross-process cache
        self._save_token_to_file_cache(self._access_token, expires_in)

        # Log token details (safely)
        logger.info(f"Access token type: {token_response.get('token_type', 'unknown')}")
        logger.info(
            f"Access token scope: {token_response.get('scope', 'not provided')}"
        )
        logger.info(f"Access token (first 20 chars): {self._access_token[:20]}...")
        logger.info(f"Full access token: {self._access_token}")

        # Decode and log token claims for debugging
        try:
            claims = self.decode_token_claims(self._access_token)
            logger.info(f"Token issuer: {claims.get('iss', 'unknown')}")
            logger.info(f"Token audience: {claims.get('aud', 'unknown')}")
            logger.info(f"Token subject: {claims.get('sub', 'unknown')}")
            if "exp" in claims:
                exp_time = time.strftime(
                    "%Y-%m-%d %H:%M:%S UTC", time.gmtime(claims["exp"])
                )
                logger.info(f"Token expires at: {exp_time}")
        except Exception as e:
            logger.warning(f"Failed to decode token claims: {e}")

        return self._access_token

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
