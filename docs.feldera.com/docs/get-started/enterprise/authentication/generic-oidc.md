# Generic OIDC Provider

This guide explains how to configure any OIDC-compliant authentication provider for Feldera Enterprise.

## Overview

Feldera supports standard OIDC/OAuth2 authentication, allowing you to integrate with any OIDC-compliant identity provider such as Auth0, Keycloak, Azure AD, Google Identity, or other enterprise identity providers.

## OIDC Application Setup

### 1. Create OIDC Application

In your OIDC provider's admin console:

1. Create a new application or client
2. Select **Single-Page Application (SPA)** as the application type
3. Fill in application details:
   - **Application name**: `Feldera` (or your preferred name)
   - **Application description**: Optional description for your organization

### 2. Configure Grant Types

Enable the following OAuth 2.0 grant types:

- **Authorization Code** grant type (required)
- **Refresh Token** (recommended for long-lived sessions)
- **PKCE** (Proof Key for Code Exchange) - highly recommended for security

### 3. Configure Redirect URLs

Add the following URLs to your OIDC application configuration:

- **Sign-in redirect URI**: `https://<your-feldera-domain>/auth/callback/`
- **Sign-out redirect URI**: `https://<your-feldera-domain>/`

:::note

**Important:** The trailing slash (`/`) in the callback URL **must be included**. Most OIDC providers require exact URL matching.

:::

### 4. Configure Scopes

Ensure the following OpenID Connect scopes are allowed:

- `openid` (required)
- `profile` (recommended)
- `email` (required)
- `offline_access` (required)

## Tenant Assignment (Optional)

Feldera supports authorization through multiple tenant assignment strategies. By default, each user gets their own individual tenant based on the `sub` claim.

### Multi-Tenant Access with Custom Claims

To enable managed tenancy, configure your OIDC provider to include custom claims in the Access token:

#### `tenants` Claim

Configure your OIDC provider to include a `tenants` claim in the **Access token**:

- **Claim name**: `tenants`
- **Token type**: Access Token
- **Value type**: Array of strings or comma-separated string
- **Value**: List of tenant names the user can access

**Example token claim:**
```json
{
  "tenants": ["engineering", "data-science", "analytics"]
}
```

When multiple tenants are configured, users can switch between tenants in the Web Console or specify the tenant using the `Feldera-Tenant` HTTP header.

## Group-Based Authorization (Optional)

To restrict access based on group membership, configure your OIDC provider to include a `groups` claim in the Access token:

- **Claim name**: `groups`
- **Token type**: Access Token
- **Value type**: Array of strings
- **Value**: List of group names the user belongs to

**Example token claim:**
```json
{
  "groups": ["feldera-users", "data-engineers", "admins"]
}
```

When `authorizedGroups` is configured in Feldera, users must have at least one matching group to access the platform.

## Configure Feldera

After setting up your OIDC provider, configure Feldera using the Helm chart values; see [examples](index.mdx#Tenant%20Assignment%20use%20cases) for common authorization scenarios.

### Configuration Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `auth.clientId` | Your OIDC application's client ID | `abc123xyz456` |
| `auth.issuer` | Your OIDC provider's issuer URL | `https://auth.example.com` |
| `authorization.authAudience` | Expected audience claim value in Access tokens | `feldera-api` |

:::tip

Consult the [main authentication documentation](index.mdx#Configuration%20options) for complete configuration options.

:::

Refer to your provider's documentation for specific setup instructions while following the general OIDC configuration pattern outlined above.
