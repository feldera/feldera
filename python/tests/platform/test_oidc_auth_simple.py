"""
Simplified OIDC Authentication Integration Tests

Tests OIDC authentication using Resource Owner Password Flow only.

Environment Variables Required:
- OIDC_TEST_ISSUER: The issuer URL for the OIDC provider
- OIDC_TEST_CLIENT_ID: Client ID for the test application
- OIDC_TEST_CLIENT_SECRET: Client secret for Resource Owner Password Flow
- OIDC_TEST_USERNAME: Username for test user
- OIDC_TEST_PASSWORD: Password for test user
"""

from .helper import (
    API_PREFIX,
    HTTPStatus,
    get,
    http_request,
)
from .oidc_test_helper import (
    skip_if_oidc_not_configured,
    get_oidc_test_helper,
)


# Skip all tests if OIDC test environment is not configured
pytestmark = skip_if_oidc_not_configured()


def test_authentication_config_discovery():
    """Test that the authentication configuration endpoint returns GenericOidc configuration"""
    helper = get_oidc_test_helper()
    assert helper is not None

    response = get("/config/authentication")
    assert response.status_code == HTTPStatus.OK

    config = response.json()
    assert "GenericOidc" in config

    oidc_config = config["GenericOidc"]
    assert "issuer" in oidc_config
    assert "client_id" in oidc_config
    assert oidc_config["issuer"] == helper.config.issuer
    assert oidc_config["client_id"] == helper.config.client_id


def test_oidc_discovery_endpoint():
    """Test that the remote OIDC provider's discovery endpoint is accessible"""
    helper = get_oidc_test_helper()
    assert helper is not None

    discovery_doc = helper.get_discovery_document()
    assert "issuer" in discovery_doc
    assert "jwks_uri" in discovery_doc
    assert "token_endpoint" in discovery_doc
    assert discovery_doc["issuer"] == helper.config.issuer


def test_obtain_access_token():
    """Test obtaining access token via Resource Owner Password Flow"""
    helper = get_oidc_test_helper()
    assert helper is not None

    token = helper.obtain_access_token()
    assert token is not None
    assert len(token) > 0

    # Verify token structure
    assert helper.validate_token_structure(token)

    # Verify claims and debug them
    claims = helper.decode_token_claims(token)
    print(f"DEBUG: All JWT claims: {claims}")
    print(f"DEBUG: Audience claim type: {type(claims.get('aud'))}")
    print(f"DEBUG: Audience claim value: {claims.get('aud')}")
    print(f"DEBUG: Issuer claim: {claims.get('iss')}")

    assert claims["iss"] == helper.config.issuer


def test_authenticated_endpoint_access():
    """Test that valid token allows access to protected endpoints"""
    helper = get_oidc_test_helper()
    assert helper is not None

    headers = helper.create_authenticated_headers()

    # Test accessing protected endpoints
    response = http_request("GET", f"{API_PREFIX}/pipelines", headers=headers)
    if response.status_code not in [HTTPStatus.OK, HTTPStatus.NOT_FOUND]:
        print(f"ERROR: Pipelines endpoint failed with status {response.status_code}")
        print(f"Response headers: {dict(response.headers)}")
        print(f"Response body: {response.text}")
    assert response.status_code in [
        HTTPStatus.OK,
        HTTPStatus.NOT_FOUND,
    ]  # Either works or empty result

    # Test accessing session config
    response = http_request("GET", f"{API_PREFIX}/config/session", headers=headers)
    if response.status_code != HTTPStatus.OK:
        print(
            f"ERROR: Session config endpoint failed with status {response.status_code}"
        )
        print(f"Response headers: {dict(response.headers)}")
        print(f"Response body: {response.text}")
    assert response.status_code == HTTPStatus.OK

    session_config = response.json()
    assert "tenant_id" in session_config


def test_unauthenticated_request_rejection():
    """Test that requests without authentication are rejected"""
    headers = {"Accept": "application/json"}

    response = http_request("GET", f"{API_PREFIX}/pipelines", headers=headers)
    assert response.status_code == HTTPStatus.UNAUTHORIZED


def test_malformed_token_rejection():
    """Test that malformed JWT tokens are rejected"""
    helper = get_oidc_test_helper()
    assert helper is not None

    malformed_tokens = helper.get_malformed_test_tokens()

    for token_type, invalid_token in malformed_tokens.items():
        headers = {
            "Authorization": f"Bearer {invalid_token}",
            "Accept": "application/json",
        }

        response = http_request("GET", f"{API_PREFIX}/pipelines", headers=headers)
        assert response.status_code == HTTPStatus.UNAUTHORIZED, (
            f"Token type {token_type} should be rejected"
        )


# Note: Pipeline CRUD operations with authentication are now tested
# by all platform tests since they use OIDC authentication when configured.
# This file focuses on OIDC-specific authentication functionality.
