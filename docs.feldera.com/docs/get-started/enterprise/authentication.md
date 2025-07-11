# Authentication

This document describes how to configure authentication providers to work with **Feldera**.

## AWS Cognito

To configure AWS Cognito as an authentication provider, follow these steps:


### Create a Cognito User Pool

In the AWS Console:

- Navigate to **Amazon Cognito** and create a new **User Pool**.
- Choose any settings appropriate to your organization (e.g., required user attributes, password policies).


### Create an App Client

Once the User Pool is created:

- Go to the **App clients** section and create a new app client.
- **Select "Single-page application (SPA)"** as the app client type.
- In the **Return URL** field, add: `https://<your-domain>/auth/callback/`

:::note

**Important:** The trailing slash (`/`) at the end of the URL **must be included**. AWS Cognito requires exact URL matching, and omitting the slash may result in redirect errors.

:::

### Configure OAuth 2.0 Settings

In the App Client's **Login Settings**:

- Add `https://<your-domain>/auth/callback/` to the **Callback URLs**.
- Enable the following **OAuth 2.0 grant types**:
  - `Authorization code grant`
  - `Implicit grant`
- Select the following **OpenID Connect scopes**:
  - `email`
  - `profile`
  - `openid`

### Set Up Domain and Branding

Go to the **App integration → Domain name** section and set up a custom domain or use the AWS-hosted one (e.g., `your-app.auth.us-west-1.amazoncognito.com`).

This domain will be used in your login and logout URLs.

### Configure Helm Chart (`values.yaml`)

In your Feldera Helm chart configuration (`values.yaml`), fill out the `auth` section with the information from the Cognito console:

```yaml
auth:
  enabled: true
  provider: "aws-cognito"
  clientId: "<your-client-id>"
  issuer: "https://cognito-idp.<region>.amazonaws.com/<user-pool-id>"
  cognitoLoginUrl: "https://<your-domain>.auth.<region>.amazoncognito.com/login?client_id=<your-client-id>&response_type=code&scope=email+openid"
  cognitoLogoutUrl: "https://<your-domain>.auth.<region>.amazoncognito.com/logout?client_id=<your-client-id>"
```

Replace all placeholders (`<your-client-id>`, `<region>`, `<user-pool-id>`, `<your-domain>`) with values from the AWS Cognito console.

| Placeholder        | Description                                                                        |
| ------------------ | ---------------------------------------------------------------------------------- |
| `<your-client-id>` | Found under **App client information** in your Cognito User Pool.                  |
| `<user-pool-id>`   | Found in the User Pool's main page.                                                |
| `<region>`         | The AWS region of your User Pool (e.g., `us-west-1`).                              |
| `<your-domain>`    | Your Cognito domain, under **Branding → Domain name**.                             |
| `issuer`           | Has the form of `https://cognito-idp.<region>.amazonaws.com/<user-pool-id>`.       |
