// https://stackoverflow.com/questions/56462796/how-to-generate-aws-exports-js-with-existing-user-pool-in-aws-cognito

export const awsAmplifyConfig = {
  Auth: {
    identityPoolId: process.env.NEXT_PUBLIC_AWS_AMPLIFY_IDENTITY_POOL_ID, // AWS > Cognito > Federated Identities > Select the identity pool > Sample code > Get AWS Credentials.
    region: process.env.NEXT_PUBLIC_AWS_AMPLIFY_REGION,
    userPoolId: process.env.NEXT_PUBLIC_AWS_AMPLIFY_USER_POOL_ID, // AWS > Cognito > User pools (Select the user pool) > User pool ID
    userPoolWebClientId: process.env.NEXT_PUBLIC_AWS_AMPLIFY_USER_POOL_WEB_CLIENT_ID // AWS > Cognito > User pools (Select the user pool) > Select App Integration tab > App client list > Select the App client name > Client ID
  }
}
