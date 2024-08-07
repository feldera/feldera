import '$lib/compositions/setupHttpClient'
import { loadAuthConfig } from '$lib/compositions/auth'
import * as AxaOidc from '@axa-fr/oidc-client'
import type { OidcUserInfo } from '@axa-fr/oidc-client'
import { fromAxaUserInfo, toAxaOidcConfig } from '$lib/compositions/@axa-fr/auth'
import { client } from '@hey-api/client-fetch'
import { base } from '$app/paths'
import { authRequestMiddleware, authResponseMiddleware } from '$lib/services/auth'
import type { AuthDetails } from '$lib/types/auth'
const { OidcClient, OidcLocation } = AxaOidc

export const ssr = false
export const trailingSlash = 'always'

export const load = async ({ fetch, url }): Promise<{ auth: AuthDetails }> => {
  if (!('window' in globalThis)) {
    return {
      auth: 'none'
    }
  }
  const authConfig = await loadAuthConfig()
  if (!authConfig) {
    return {
      auth: 'none'
    }
  }
  const axaOidcConfig = toAxaOidcConfig(authConfig.oidc)
  const oidcClient = OidcClient.getOrCreate(() => fetch, new OidcLocation())(axaOidcConfig)
  const href = url.href
  const result: AuthDetails = await oidcClient.tryKeepExistingSessionAsync().then(async () => {
    if (href.includes(axaOidcConfig.redirect_uri)) {
      oidcClient.loginCallbackAsync().then(() => {
        window.location.href = `${base}/`
      })
      // loading...
    }

    let tokens = oidcClient.tokens

    if (!tokens) {
      return {
        login: async () => {
          await oidcClient.loginAsync('/')
        }
      }
    }

    const userInfo = await oidcClient.userInfoAsync()

    client.interceptors.request.use(authRequestMiddleware)
    client.interceptors.response.use(authResponseMiddleware)
    const nonce:
      | {
          nonce: string
        }
      | {} = ((nonce) => (nonce ? { nonce } : {}))(
      (axaOidcConfig.storage ?? sessionStorage).getItem('oidc.nonce.default')
    )
    return {
      logout: ({ callbackUrl }) =>
        oidcClient.logoutAsync(callbackUrl, {
          ...authConfig.logoutExtras,
          ...nonce // With AWS Cognito, when logging out and logging in via thrird party IDP (e.g. Google) - nonce is required
        }),
      userInfo,
      profile: fromAxaUserInfo(userInfo),
      accessToken: tokens.accessToken // Only used in HTTP requests that cannot be handled with the global HTTP client instance from @hey-api/client-fetch
    }
  })
  return {
    auth: result
  }
}
