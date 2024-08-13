import { type OidcConfig, type UserProfile } from '$lib/compositions/auth'
import { type OidcConfiguration, type OidcUserInfo } from '@axa-fr/oidc-client'
import * as OidcClient from '@axa-fr/oidc-client'
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
  authority_configuration: {
    authorization_endpoint: c.metadata.authorization_endpoint,
    token_endpoint: c.metadata.token_endpoint,
    revocation_endpoint: c.metadata.revocation_endpoint,
    end_session_endpoint: c.metadata.end_session_endpoint,
    userinfo_endpoint: c.metadata.userinfo_endpoint,
    // check_session_iframe: undefined,
    issuer: c.metadata.issuer
  },
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
  // storage: undefined,
  // monitor_session: undefined,
  // token_renew_mode: undefined,
  logout_tokens_to_invalidate: []
  // demonstrating_proof_of_possession: undefined,
  // demonstrating_proof_of_possession_configuration: undefined,
  // preload_user_info: undefined,
})

export const fromAxaUserInfo = (userInfo: OidcUserInfo): UserProfile => ({
  id: userInfo.sub,
  name: userInfo.preferred_username || userInfo.name,
  email: userInfo.email,
  image: userInfo.picture
})
