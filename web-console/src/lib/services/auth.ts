import * as AxaOidc from '@axa-fr/oidc-client'
const { OidcClient, OidcLocation } = AxaOidc

export const authRequestMiddleware = (request: Request) => {
  const oidcClient = OidcClient.get()
  if (oidcClient.tokens?.accessToken) {
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
    // Use getValidTokenAsync which only renews tokens if they're actually expired,
    // preserving ID token (thus, id_token_hint on logout) when 401 is due to authorization issues rather than token expiry
    const validToken = await client.getValidTokenAsync()
    if (validToken?.isTokensValid) {
      return fetch(request)
    }

    console.error('Unable to extend user session. refresh_token was probably not issued.')
  }
  return response
}
