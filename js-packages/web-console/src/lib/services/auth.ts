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
 * Error interceptor that tags the thrown error with HTTP status info so
 * downstream consumers can branch on e.g. 401. Mutates in place instead of
 * spreading — spreading an Error drops its non-enumerable `message`/`stack`
 * and reshapes a TypeError into a plain object, which would break downstream
 * `instanceof TypeError` network-error detection. `response` is `undefined`
 * when the interceptor fires from the fetch-failure path (network error).
 */
export const errorResponseMiddleware = (error: unknown, response: Response | undefined) => {
  if (error && typeof error === 'object') {
    try {
      ;(error as any).response = response
      if (response) (error as any).status = response.status
      return error
    } catch {
      // Frozen / non-extensible — fall through to a fresh object.
    }
  }
  return {
    message: typeof error === 'string' ? error : String(error),
    response,
    status: response?.status
  }
}

/**
 * Start an OIDC re-authentication flow.
 *
 * Stashes the current URL under `redirect_to` in session storage so the
 * `onAfterLogin` hook in `routes/+layout.ts` can restore it via `goto` once
 * the provider callback completes. Only writes the key if it isn't already
 * set, so the *first* call (which captures the user's actual page) wins —
 * the fallback navigation below re-invokes this function from `/`, and we
 * must not overwrite the original target `redirect_to` with `/`.
 *
 * If the OIDC singleton has not been initialized yet — e.g. the backend was
 * unauthenticated when the root layout first loaded and has since flipped to
 * requiring auth — falls back to a full navigation to `/`, which re-enters
 * the root layout. The fresh `initAuth` will pick up the new auth config,
 * register the OIDC client, and the (authenticated) layout's `auth.login`
 * will route back through this function — this time with the singleton
 * available — and complete the redirect to the provider.
 */
export const triggerOidcLogin = async (): Promise<void> => {
  if (!window.sessionStorage.getItem('redirect_to')) {
    window.sessionStorage.setItem('redirect_to', window.location.href)
  }
  let oidcClient
  try {
    oidcClient = OidcClient.get()
  } catch {
    window.location.href = '/'
    return
  }
  await oidcClient.loginAsync('/')
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
