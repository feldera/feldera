import * as AxaOidc from '@axa-fr/oidc-client'
const { OidcClient, OidcLocation } = AxaOidc

export const authRequestMiddleware = (request: Request) => {
  const oidcClient = OidcClient.get()
  if (oidcClient.tokens.accessToken) {
    request.headers.set('Authorization', `Bearer ${oidcClient.tokens.accessToken}`)
  }
  return request
}

/**
 * In case of auth error try to refresh tokens and re-fetch original request
 */
export const authResponseMiddleware = async (response: Response, request: Request) => {
  if (response.status === 401) {
    const client = OidcClient.get()
    await client.renewTokensAsync()
    return fetch(request)
  }
  return response
}
