# Okta SSO

This guide explains how to configure Okta as an authentication provider for Feldera Enterprise.

## Overview

Okta integration allows organizations to leverage existing and dedicated Okta user groups for access control.

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

### 4. Create a Custom Authorization Server

After creating the app, to take advantage of the flexible tenant-based authorization models you need to set up a custom authorization server that issues custom claims in the Access OIDC token based on Okta user groups.

## Custom Authorization Server Setup

### 1. Create Authorization Server

In your Okta Admin Dashboard:

1. Navigate to **Security** → **API** → **Authorization Servers**
2. Click **Add Authorization Server**
3. Fill in server details:
   - **Name**: `Feldera`
   - **Audience**: `feldera-api`
   - **Description**: `Authorization server for Feldera tenant and group claims`

:::note

By default, Feldera expects `feldera-api` audience value. If you want to use the default value from Okta you need to specify it in the `authAudience` option in your Helm config.

:::

### 2. Set up an Access Policy

Ensure there is a policy that will allow user authentication. For a simple setup:

1. Press "Add New Access Policy", fill the name and description, in the selector "Assign to" pick "The following clients:" and find the name of the Feldera application(s) you created before, and confirm the creation.
2. Press "Add rule", and ensure that the following options are selected:
   - IF Grant type is - "Authorization Code"
   - AND Scopes requested - "Any scopes"

   The rest can be configured as needed.

### 3. Configure Custom Claims

You can take advantage of the supported authorization models by properly configuring the custom OIDC claims.

## Tenant Assignment with custom claims

Feldera supports multiple authorization use-cases through [managed tenancy](index.mdx#Managed%20Tenancy). You can choose between the supported tenant claims to implement the appropriate authorization scenario. Navigate to the **Claims** tab in the Custom Authorization Server to configure one of:

### `tenants` claim

Example configuration for the `tenants` claim that assigns a single (randomly selected) user group name prefixed with "feldera_" as the tenant name:

   - **Name**: `tenants`
   - **Include in token type**: `Access Token`
   - **Value type**: `Expression`
   - **Value**: `{user.getGroups({"group.profile.name": "feldera_", "operator": "STARTS_WITH"})[0].name}`
   - **Include in**: `Any scope`

In the above example each user should only have one user group assigned to them that satisfies the condition in the expression value.

Example configuration for the `tenants` claim that uses all user groups prefixed with "feldera_" as the list of tenants:

   - **Name**: `tenants`
   - **Include in token type**: `Access Token`
   - **Value type**: `Expression`
   - **Value**: `user.getGroups({"group.profile.name": "feldera_", "operator": "STARTS_WITH"}).![name]`
   - **Include in**: `Any scope`

## Authorization through group membership with a custom claim

Feldera can restrict access based on Okta group membership using the `groups` claim configured in your custom authorization server. This claim is orthogonal to tenant assignment.

Example configuration for the `groups` claim that communicates all groups that the user is a part of:
   - **Name**: `groups`
   - **Include in token type**: `Access Token`
   - **Value type**: `Groups`
   - **Value**: Select appropriate group filter or use all-inclusive regex `.*`
   - **Include in**: `Any scope`

## Configure Feldera

Consult the documentation for [configuring the authentication](index.mdx#Configuration%20options) and [examples for common use-cases](index.mdx#Tenant%20Assignment%20use%20cases) to configure Feldera to authorize users properly.

One example of the Feldera Helm chart configuration for managed tenancy with Okta:

```yaml
auth:
  enabled: true
  provider: "okta"
  clientId: "0oa1a2b3c4d5e6f7g8h9"
  issuer: "https://dev-12345.okta.com/oauth2/aus1a2b3c4d5e6f7g8h9"

authorization:
  individualTenant: false
  authAudience: "0oa1a2b3c4d5e6f7g8h9"
```
