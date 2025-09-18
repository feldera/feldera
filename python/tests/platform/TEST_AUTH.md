# Platform Test Authentication

This document describes how to run Feldera platform tests with different authentication methods.

## Overview

Platform tests support **two authentication methods** depending on configuration:

1. **OIDC Authentication** (preferred when configured)
2. **API Key Authentication** (fallback)
3. **No Authentication** (development fallback)

The authentication method is **automatically selected** based on available environment variables.

## Authentication Methods

### 1. OIDC Authentication (Preferred)

Uses Resource Owner Password Flow to obtain JWT tokens from your OIDC provider.

**Environment Variables:**
```bash
export OIDC_TEST_ISSUER="https://your-oidc-provider.com"
export OIDC_TEST_CLIENT_ID="your-client-id"
export OIDC_TEST_CLIENT_SECRET="your-client-secret"
export OIDC_TEST_USERNAME="testuser@example.com"
export OIDC_TEST_PASSWORD="test-password"
```

### 2. API Key Authentication (Fallback)

Uses a static API key for authentication.

**Environment Variables:**
```bash
export FELDERA_API_KEY="your-api-key"
```

### 3. No Authentication (Development)

If neither OIDC nor API key is configured, tests run without authentication.

## Local Testing Instructions

Assumes a local Feldera instance running on `localhost:8080`.

### Option 1: OIDC Authentication

1. **Configure your OIDC provider** (e.g., Okta, Auth0)
2. **Set environment variables:**
   ```bash
   export OIDC_TEST_ISSUER="https://your-provider.com"
   export OIDC_TEST_CLIENT_ID="your-client-id"
   export OIDC_TEST_CLIENT_SECRET="your-client-secret"
   export OIDC_TEST_USERNAME="testuser@example.com"
   export OIDC_TEST_PASSWORD="test-password"
   export FELDERA_HOST="http://localhost:8080"
   ```

3. **Start pipeline-manager with OIDC authentication:**
   ```bash
   ./pipeline-manager \
     --auth-provider generic-oidc \
     --auth-issuer "$OIDC_TEST_ISSUER" \
     --auth-client-id "$OIDC_TEST_CLIENT_ID"
   ```

4. **Run platform tests:**
   ```bash
   cd python
   uv run pytest tests/platform -v
   ```

### Option 2: API Key Authentication

1. **Create an API key** via Feldera UI or API
2. **Set environment variables:**
   ```bash
   export FELDERA_API_KEY="your-api-key"
   export FELDERA_HOST="http://localhost:8080"
   ```

3. **Start pipeline-manager** (default configuration):
   ```bash
   ./pipeline-manager
   ```

4. **Run platform tests:**
   ```bash
   cd python
   uv run pytest tests/platform -v
   ```

### Option 3: No Authentication (Development)

1. **Set environment variables:**
   ```bash
   export FELDERA_HOST="http://localhost:8080"
   ```

2. **Start pipeline-manager** (default configuration):
   ```bash
   ./pipeline-manager
   ```

3. **Run platform tests:**
   ```bash
   cd python
   uv run pytest tests/platform -v
   ```

## Running Specific Authentication Tests

### OIDC-Specific Tests

Tests OIDC authentication functionality specifically:

```bash
cd python
uv run pytest tests/platform/test_oidc_auth_simple.py -v
```

**Note:** These tests require OIDC configuration and will be skipped if not available.

### General Platform Tests

Tests all platform functionality with whatever authentication is configured:

```bash
cd python
uv run pytest tests/platform -v
```

## Authentication Priority

The platform tests automatically select authentication in this order:

1. **OIDC Token** (if all OIDC environment variables are set)
2. **API Key** (if `FELDERA_API_KEY` is set)
3. **No Authentication** (fallback for development)

## CI/CD Configuration

### Repository Variables (Public)
```
OIDC_TEST_ISSUER=https://your-oidc-provider.com
OIDC_TEST_CLIENT_ID=your-client-id
```

### Repository Secrets (Private)
```
OIDC_TEST_CLIENT_SECRET=your-client-secret
OIDC_TEST_USERNAME=testuser@example.com
OIDC_TEST_PASSWORD=test-password
```

## Test Files

- `helper.py` - Main test utilities with unified authentication support
- `test_oidc_auth_simple.py` - OIDC-specific authentication tests
- `testutils_oidc.py` - OIDC token management utilities
- All other `test_*.py` files - Platform tests that use unified authentication