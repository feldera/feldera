import '$lib/compositions/setupHttpClient'
import { loadAuthConfig, type UserProfile } from '$lib/compositions/auth'
import * as AxaOidc from '@axa-fr/oidc-client'
import type { OidcUserInfo } from '@axa-fr/oidc-client'
import { fromAxaUserInfo, toAxaOidcConfig } from '$lib/compositions/@axa-fr/auth'
import { client } from '@hey-api/client-fetch'
import { base } from '$app/paths'
import { authRequestMiddleware, authResponseMiddleware } from '$lib/services/auth'
const { OidcClient, OidcLocation } = AxaOidc

export const ssr = false
export const trailingSlash = 'always'

type AuthDetails =
  | 'none'
  | {
      login: () => Promise<void>
    }
  | {
      logout: (params: { callbackUrl: string }) => Promise<void>
      userInfo: OidcUserInfo
      profile: UserProfile
      accessToken: string
    }

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
  const oidcFetch = oidcClient.fetchWithTokens(fetch)
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
    return {
      logout: ({ callbackUrl }) => oidcClient.logoutAsync(callbackUrl, authConfig.logoutExtras),
      userInfo,
      profile: fromAxaUserInfo(userInfo),
      accessToken: tokens.accessToken // Only used in HTTP requests that cannot be handled with the global HTTP client instance from @hey-api/client-fetch
    }
  })
  return {
    auth: result
  }
}
