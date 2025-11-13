import * as AxaOidc from '@axa-fr/oidc-client'
const { OidcClient } = AxaOidc

let selectedTenant: string | undefined =
  ('window' in globalThis ? window.localStorage.getItem('session/selected_tenant') : undefined) ??
  undefined

export const getSelectedTenant = () => {
  return selectedTenant
}

export const setSelectedTenant = (tenant?: string) => {
  selectedTenant = tenant
  if (tenant === undefined) {
    window.localStorage.removeItem('session/selected_tenant')
    return
  }
  window.localStorage.setItem('session/selected_tenant', tenant)
}

/**
 * Core authentication request logic that adds auth headers to requests.
 * Can be used both as a middleware interceptor and directly in fetch operations.
 */
export const applyAuthToRequest = (request: Request): Request => {
  try {
    const oidcClient = OidcClient.get()
    if (oidcClient.tokens?.accessToken) {
      request.headers.set('Authorization', `Bearer ${oidcClient.tokens.accessToken}`)
      const tenant = getSelectedTenant()
      if (tenant) {
        request.headers.set('Feldera-Tenant', tenant)
      }
    }
  } catch {
    // OidcClient.get() throws if not initialized, which is fine for unauthenticated usage
  }
  return request
}

/**
 * Middleware interceptor for @hey-api/client-fetch
 */
export const authRequestMiddleware = (request: Request) => {
  return applyAuthToRequest(request)
}

/**
 * Core authentication response logic that handles 401 errors by refreshing tokens.
 * Can be used both as a middleware interceptor and directly in fetch operations.
 *
 * @param response The HTTP response
 * @param request The original request (used for retry after token refresh)
 * @param fetchFn The fetch function to use for retrying (defaults to globalThis.fetch)
 */
export const handleAuthResponse = async (
  response: Response,
  request: Request,
  fetchFn: typeof globalThis.fetch = globalThis.fetch
): Promise<Response> => {
  if (response.status === 401) {
    try {
      const client = OidcClient.get()
      // Use getValidTokenAsync which only renews tokens if they're actually expired,
      // preserving ID token (thus, id_token_hint on logout) when 401 is due to authorization issues rather than token expiry
      const validToken = await client.getValidTokenAsync()
      if (validToken?.isTokensValid) {
        return fetchFn(request)
      }

      console.error('Unable to extend user session. refresh_token was probably not issued.')
    } catch {
      // OidcClient.get() throws if not initialized
    }
  }
  return response
}

/**
 * Middleware interceptor for @hey-api/client-fetch
 * In case of auth error try to refresh tokens and re-fetch original request
 */
export const authResponseMiddleware = async (response: Response, request: Request) => {
  return handleAuthResponse(response, request, fetch)
}

/**
 * Gets the authorization and tenant headers for authenticated requests.
 * Returns an empty object if not authenticated.
 */
export const getAuthorizationHeaders = async (): Promise<Record<string, string>> => {
  try {
    const oidcClient = OidcClient.get()
    const tokens = await oidcClient.getValidTokenAsync()
    const headers: Record<string, string> = {}

    if (tokens?.tokens?.accessToken) {
      headers['Authorization'] = `Bearer ${tokens.tokens.accessToken}`
      const tenant = getSelectedTenant()
      if (tenant) {
        headers['Feldera-Tenant'] = tenant
      }
    }

    return headers
  } catch {
    return {}
  }
}
