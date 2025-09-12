# Okta SSO

This guide explains how to configure Okta as an authentication provider for Feldera Enterprise.

## Overview

Okta integration allows organizations to:
- **Add Feldera as an app** in their Okta portal for centralized access management
- **Leverage existing user groups** for tenant assignment and access control
- **Support multiple deployment models** from individual tenancy to enterprise B2B

## Okta Application Setup

### 1. Create Okta Application

In your Okta Admin Dashboard:

1. Navigate to **Applications** → **Create App Integration**
2. Select **OIDC - OpenID Connect** as the sign-in method
3. Choose **Single-Page Application (SPA)** as the application type
4. Fill in application details:
   - **App integration name**: `Feldera`
   - **App logo**: Upload your organization's logo (optional)

### 2. Configure Grant Types

In the **General Settings** tab:

- Enable **Authorization Code** grant type
- Ensure **Refresh Token** is enabled for long-lived sessions
- **PKCE** is automatically enabled for SPA applications (recommended for security)

### 3. Configure Redirect URLs

Add the following URLs to your Okta application:

- **Sign-in redirect URIs**: `https://<your-feldera-domain>/auth/callback/`
- **Sign-out redirect URIs**: `https://<your-feldera-domain>/`

:::note

**Important:** The trailing slash (`/`) in the callback URL **must be included**. Okta requires exact URL matching.

:::

You can skip Trusted Origins configuration.

### 5. Create a Custom Authorization Server

After creating the app, you need to set up a custom authorization server to provide tenant and group claims.

## Custom Authorization Server Setup

To provide custom claims in the Access token for tenant assignment and group membership authorization, you must create a custom authorization server in Okta:

### 1. Create Authorization Server

In your Okta Admin Dashboard:

1. Navigate to **Security** → **API** → **Authorization Servers**
2. Click **Add Authorization Server**
3. Fill in server details:
   - **Name**: `Feldera`
   - **Audience**: `feldera-api`
   - **Description**: `Authorization server for Feldera tenant and group claims`

### 2. Configure Custom Claims

Let's assume you want to differentiate user groups that should be used for tenancy assignment with `feldera_`.

In your custom authorization server:

1. Navigate to **Claims** tab
2. Click **Add Claim** to create the **tenant** claim:
   - **Name**: `tenant`
   - **Include in token type**: `Access Token`
   - **Value type**: `Expression`
   - **Value**: `user.getGroups({"group.profile.name": "feldera_", "operator": "STARTS_WITH"})[0].name`
   - **Include in**: `Any scope`

3. Click **Add Claim** to create the **groups** claim:
   - **Name**: `groups`
   - **Include in token type**: `Access Token`
   - **Value type**: `Groups`
   - **Value**: Select appropriate group filter or use regex `.*`
   - **Include in**: `Any scope`

## Tenant Assignment with a custom claim

Feldera supports [multiple tenant assignment strategies](index.mdx#Tenant Assignment Strategies).

### Individual Tenancy (Development)

No additional Okta configuration required.

### Organization Tenancy (One tenant per org)

No additional Okta configuration required.

### Custom Tenant Claims (Enterprise)

Use the custom authorization server configured above to assign users to specific tenants based on their group membership.

Below is an example configuration that uses the tenant claim based on the user's Feldera-specific group membership. Users should be assigned to groups with a `feldera_` prefix to distinguish them from other organizational groups. For example:
- User in group `feldera_engineering` → `tenant` claim = `feldera_engineering`
- User in group `feldera_marketing` → `tenant` claim = `feldera_marketing`
- User in group `feldera_customer_acme` → `tenant` claim = `feldera_customer_acme`

#### Step 1: Create Tenant Groups

In Okta **Directory** → **Groups**:

1. Create groups using the `feldera_` prefix followed by the tenant name:
2. Examples:
   - `feldera_engineering`
   - `feldera_marketing`
   - `feldera_customer_acme`
   - `feldera_customer_globex`

#### Step 2: Assign Users to Groups

Add users to appropriate Feldera tenant groups based on their access requirements. Users can belong to multiple groups, but only groups with the `feldera_` prefix will be considered for tenant assignment.

#### Step 3: Configuring Feldera instance

The `tenant` claim is always respected, so you only need to disable individual tenancy:

**Feldera Configuration:**
```bash
pipeline-manager ...\
  --auth-provider=generic-oidc \
  --individual-tenant=false
```

## Group membership authorization with a custom claim

Feldera can restrict access based on Okta group membership using the `groups` claim configured in your custom authorization server. This is separate from tenant assignment and controls who can access Feldera at all.

The groups claim is automatically populated in the custom authorization server setup above. You can adjust the group filter in the claim configuration if needed.

### Configure Feldera Authorization

```bash
# Require users to belong to specific groups
pipeline-manager \
  --auth-provider=generic-oidc \
  --authorized-groups=feldera-users,analytics-team
```

Users must belong to at least one of the specified groups to access Feldera. If `--authorized-groups` is not specified, no group restrictions apply.

## Environment Variables

Configure the following environment variables for your Feldera deployment:

### Required Variables

```bash
# Okta OIDC configuration
AUTH_ISSUER=https://<your-okta-domain>/oauth2/<custom-auth-server-id>
AUTH_CLIENT_ID=<your-client-id>
```

### Optional Variables

```bash
# Custom authorization server (if not using default)
AUTH_ISSUER=https://<your-okta-domain>/oauth2/<custom-auth-server-id>
```

## Helm Chart Configuration

Configure your Feldera Helm chart (`values.yaml`) with Okta settings:

```yaml
auth:
  enabled: true
  provider: "okta"
  clientId: "<your-client-id>"
  issuer: "https://<your-okta-domain>/oauth2/<custom-auth-server-id>"

# Tenant assignment strategy
pipelineManager:
  extraArgs:
    - "--auth-provider=generic-oidc"
    - "--issuer-tenant=true"        # Enable organization tenancy
    - "--individual-tenant=false"   # Disable individual tenancy
```

Replace the placeholders:

| Placeholder | Description | Example |
|------------|-------------|---------|
| `<your-okta-domain>` | Your Okta organization domain | `dev-12345.okta.com` |
| `<your-client-id>` | Application client ID from Okta | `0oa1a2b3c4d5e6f7g8h9` |
| `<auth-server-id>` | Custom authorization server ID (optional) | `aus1a2b3c4d5e6f7g8h9` |

## Multi-Customer B2B Setup

For B2B deployments where multiple customer organizations use the same Feldera instance:

### 1. Customer Onboarding Process

When a new B2B customer wants to add Feldera:

1. **Customer** creates Feldera application in their Okta portal (following steps above)
2. **Customer** configures their Okta domain and client ID
3. **Customer** provides their Okta issuer URL to your team
4. **You** add customer's issuer to your Feldera deployment configuration

### 2. Multi-Issuer Configuration

Configure Feldera to accept tokens from multiple Okta organizations:

```bash
# Example supporting multiple customers
AUTH_ISSUER=https://customer1.okta.com/oauth2/default,https://customer2.okta.com/oauth2/default
```

### 3. Automatic Tenant Assignment

With `--issuer-tenant=true`, each customer automatically gets their own tenant:
- Customer 1 users (`customer1.okta.com`) → `customer1` tenant
- Customer 2 users (`customer2.okta.com`) → `customer2` tenant

## Troubleshooting

### Common Issues

#### "No valid tenant found" Error
- **Cause**: User doesn't have required tenant assignment
- **Solution**:
  - Check tenant group membership in Okta
  - Verify tenant claim configuration
  - Ensure `--individual-tenant=true` if using individual tenancy

#### "Invalid audience" Error
- **Cause**: Client ID mismatch between Okta app and Feldera config
- **Solution**: Verify `AUTH_CLIENT_ID` matches Okta application client ID

#### "Invalid issuer" Error
- **Cause**: Issuer URL mismatch
- **Solution**: Verify `AUTH_ISSUER` matches Okta authorization server URL

For additional help, consult the [Okta Developer Documentation](https://developer.okta.com/docs/) or contact your Feldera support team.