# Python Tests

This directory contains the comprehensive test suite for the Feldera platform.

## Authentication Setup for Platform Tests (`python/`)

This directory contains integration tests for the Feldera Python SDK and platform integration.

### OIDC Authentication (Recommended for CI)

Platform tests in the `platform/` subdirectory support OIDC authentication via environment variables. This is the primary authentication method used in CI environments.

#### Environment Variables

```bash
export OIDC_TEST_ISSUER="https://your-oidc-provider.com"
export OIDC_TEST_CLIENT_ID="your-client-id"
export OIDC_TEST_CLIENT_SECRET="your-client-secret"
export OIDC_TEST_USERNAME="testuser@example.com"
export OIDC_TEST_PASSWORD="test-password"
export OIDC_TEST_SCOPE="openid profile email"  # Optional, defaults shown
```

#### Token Caching Architecture

The OIDC authentication system uses **pytest-xdist hooks** and **environment variables** to guarantee exactly one ROPG authentication request per test session, regardless of the number of parallel workers (`pytest -n 24`).

##### Master-Worker Token Distribution

The implementation leverages pytest-xdist's master/worker communication and cross-process environment variables:

- **Master Node Only**: OIDC token fetching occurs exclusively on the pytest master node via `pytest_configure` hook
- **Environment Variable Storage**: Token data is stored in `FELDERA_PYTEST_OIDC_TOKEN` environment variable for cross-process access
- **Zero Race Conditions**: No file locking or inter-process synchronization needed - environment variables are inherited by child processes
- **Guaranteed Once-Only**: Exactly one auth request per test session, even with unlimited parallel workers

This completely eliminates auth server rate limiting issues and cross-process token sharing problems that occurred with previous approaches.

##### Authentication Flow

1. **Master Hook Execution**: `pytest_configure()` runs only on master node and fetches OIDC token once
2. **Environment Storage**: Token data is base64-encoded and stored in `FELDERA_PYTEST_OIDC_TOKEN` environment variable
3. **Cross-Process Access**: All worker processes inherit the environment variable automatically
4. **Token Retrieval**: `obtain_access_token()` reads and parses token from environment variable
5. **Fail-Fast**: Authentication failures prevent all tests from running with clear error messages

##### Implementation Details

The OIDC token caching system uses pytest-xdist hooks and environment variables for cross-process token sharing:

- **Master-Only Fetching**: `pytest_configure()` hook fetches OIDC token once on master node
- **Environment Storage**: Token cached in `FELDERA_PYTEST_OIDC_TOKEN` with base64 encoding
- **Cross-Process Access**: Worker processes inherit environment variable automatically
- **Session Validation**: `oidc_token_fixture` verifies token setup with 30-second expiration buffer
- **Header Integration**: `http_request()` merges authentication headers with custom headers

**Key Components**:
- **`pytest_configure()` (conftest.py)**: Master-only hook that fetches OIDC token once and stores in environment
- **`pytest_configure_node()` (conftest.py)**: xdist hook that passes token data via workerinput (backup mechanism)
- **`oidc_token_fixture()` (conftest.py)**: Session fixture that verifies token setup
- **`obtain_access_token()` (testutils_oidc.py)**: Returns token from environment variable or fails fast
- **`http_request()` (helper.py)**: Merges authentication headers with custom headers for ingress/egress requests
- **Reusable Token Functions (testutils_oidc.py)**:
  - `setup_token_cache()`: High-level function that sets up token caching (used by both pytest and demo runners)
  - `get_cached_token_from_env()`: Retrieves and validates cached tokens
  - `parse_cached_token()`: Parses base64-encoded token data
  - `is_token_valid()`: Checks token expiration with configurable buffer
  - `encode_token_for_env()`: Encodes tokens for environment storage

### API Key Authentication (Fallback)

If OIDC environment variables are not configured, tests fall back to API key authentication:

```bash
export FELDERA_API_KEY="your-api-key"
```

### Usage in Tests

Platform tests automatically detect and use the appropriate authentication method via `_base_headers()` helper function.

Authentication priority:
1. **OIDC** (if `OIDC_TEST_ISSUER` and related vars are set)
2. **API Key** (if `FELDERA_API_KEY` is set)
3. **No Auth** (for local testing without authentication)

### CI Configuration

In GitHub Actions workflows, OIDC authentication is configured via repository variables and secrets:

```yaml
env:
  OIDC_TEST_ISSUER: ${{ vars.OIDC_TEST_ISSUER }}
  OIDC_TEST_CLIENT_ID: ${{ vars.OIDC_TEST_CLIENT_ID }}
  OIDC_TEST_CLIENT_SECRET: ${{ secrets.OIDC_TEST_CLIENT_SECRET }}
  OIDC_TEST_USERNAME: ${{ secrets.OIDC_TEST_USERNAME }}
  OIDC_TEST_PASSWORD: ${{ secrets.OIDC_TEST_PASSWORD }}
```

This ensures consistent authentication across both "Runtime Integration Tests" and "Platform Integration Tests (OSS Docker Image)" workflows that run in parallel.

### OIDC Usage Beyond Testing

The OIDC infrastructure is designed to be reusable outside of the test suite:

#### Demo Runners and External Tools

External tools can reuse the OIDC infrastructure by calling `setup_token_cache()` followed by `_get_effective_api_key()` to get cached tokens or fallback API keys.

#### Token Caching for Multiple Processes

The token caching system is designed to work across multiple processes:

1. **First Process**: Calls `setup_token_cache()` → fetches and caches token in environment
2. **Subsequent Processes**: Call `setup_token_cache()` → reuses cached token if still valid
3. **Automatic Refresh**: Fetches new token only when cached token expires

This pattern is used by:
- **Pytest Test Runs**: Master node fetches token, workers reuse it
- **Demo Runners**: `crates/pipeline-manager/demos/run.py` uses the same caching mechanism
- **CI Workflows**: Multiple demos in sequence reuse the same token