import type { OidcConfiguration, OidcUserInfo } from '@axa-fr/oidc-client'
import * as OidcClient from '@axa-fr/oidc-client'
import type { OidcConfig } from '#lib/compositions/auth.ts'
import type { UserProfile } from '#lib/types/auth.ts'

const { TokenAutomaticRenewMode } = OidcClient

export const toAxaOidcConfig = (c: OidcConfig): OidcConfiguration => ({
  client_id: c.client_id,
  redirect_uri: c.redirect_uri,
  // silent_redirect_uri: undefined,
  // silent_login_uri: undefined,
  // silent_login_timeout: undefined,
  scope: c.scope,
  authority: c.authority,
  // authority_time_cache_wellknowurl_in_second: undefined,
  // authority_timeout_wellknowurl_in_millisecond: undefined,
  authority_configuration: c.authority_configuration
    ? {
        authorization_endpoint: c.authority_configuration.authorization_endpoint,
        token_endpoint: c.authority_configuration.token_endpoint,
        revocation_endpoint: c.authority_configuration.revocation_endpoint,
        userinfo_endpoint: c.authority_configuration.userinfo_endpoint,
        end_session_endpoint: c.authority_configuration.end_session_endpoint,
        // check_session_iframe: undefined,
        issuer: c.authority_configuration.issuer
      }
    : undefined,
  // refresh_time_before_tokens_expiration_in_second: undefined,
  // token_automatic_renew_mode: TokenAutomaticRenewMode.AutomaticOnlyWhenFetchExecuted,
  // token_request_timeout: undefined,
  // service_worker_relative_url: undefined,
  // service_worker_register: undefined,
  // service_worker_keep_alive_path: undefined,
  // service_worker_activate: undefined,
  // service_worker_only: undefined,
  // service_worker_convert_all_requests_to_cors: undefined,
  // service_worker_update_require_callback: undefined,
  // extras: undefined,
  // token_request_extras: undefined,
  storage: c.storage,
  // monitor_session: undefined,
  // token_renew_mode: 'access_token_or_id_token_invalid' // "access_token_or_id_token_invalid" (default), "access_token_invalid", "id_token_invalid"
  // Revoke the refresh token on sign-out. Sign-out still completes if the revocation fails.
  logout_tokens_to_invalidate: ['refresh_token']
  // demonstrating_proof_of_possession: undefined,
  // demonstrating_proof_of_possession_configuration: undefined,
  // preload_user_info: undefined,
})

/**
 * Wrap the `fetch` of the OIDC client so that the client does not retry a token revocation
 * request that timed out. Sign-out waits for the revocation. The client aborts each attempt after
 * 10 s and retries an attempt that fails with an `AbortError` up to 2 times, and it has no option
 * to change this. A provider that does not answer would delay sign-out by about 30 s. The wrapper
 * rejects an aborted revocation request with a `TimeoutError`, which the client does not retry,
 * so sign-out waits at most 10 s.
 *
 * The client does not expose the revocation endpoint that it reads from the discovery document,
 * so the wrapper detects a revocation request by its `token_type_hint` parameter (RFC 7009).
 * The client sends no other request with this parameter.
 */
export const withoutRevocationRetry = (
  fetchFn: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>
): typeof globalThis.fetch => {
  const f = (input: RequestInfo | URL, init?: RequestInit) =>
    fetchFn(input, init).catch((e) => {
      if (isRevocationRequest(init) && e instanceof Error && e.name === 'AbortError') {
        throw new DOMException('Token revocation timed out', 'TimeoutError')
      }
      throw e
    })
  return Object.assign(f, { preconnect: globalThis.fetch.preconnect })
}

const isRevocationRequest = (init: RequestInit | undefined) =>
  typeof init?.body === 'string' && new URLSearchParams(init.body).has('token_type_hint')

export const fromAxaUserInfo = (userInfo: OidcUserInfo): UserProfile => ({
  id: userInfo.sub,
  name: userInfo.preferred_username || userInfo.name,
  email: userInfo.email,
  picture: userInfo.picture
})

/** The `auth_time` claim of a parsed token payload, in seconds. */
export const authTimeOf = (payload: unknown): number | undefined => {
  const authTime = (payload as { auth_time?: unknown } | null | undefined)?.auth_time
  return typeof authTime === 'number' ? authTime : undefined
}
