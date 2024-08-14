import '$lib/compositions/setupHttpClient'
import { loadAuthConfig } from '$lib/compositions/auth'
import * as AxaOidc from '@axa-fr/oidc-client'
import { fromAxaUserInfo, toAxaOidcConfig } from '$lib/compositions/@axa-fr/auth'
import { client } from '@hey-api/client-fetch'
import { base } from '$app/paths'
import { authRequestMiddleware, authResponseMiddleware } from '$lib/services/auth'
import type { AuthDetails } from '$lib/types/auth'
import { goto } from '$app/navigation'
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
  return axaOidcAuth({
    oidcConfig: axaOidcConfig,
    logoutExtras: authConfig.logoutExtras,
    onBeforeLogin: () => window.sessionStorage.setItem('redirect_to', window.location.href),
    onAfterLogin: () => {
      const redirectTo = window.sessionStorage.getItem('redirect_to')
      if (!redirectTo) {
        return
      }
      window.sessionStorage.removeItem('redirect_to')
      goto(redirectTo)
    }
  })
}

const axaOidcAuth = async (params: {
  oidcConfig: AxaOidc.OidcConfiguration,
  logoutExtras?: AxaOidc.StringMap,
  onBeforeLogin?: () => void,
  onAfterLogin?: () => void}) => {
  const oidcClient = OidcClient.getOrCreate(() => fetch, new OidcLocation())(params.oidcConfig)
  const href = window.location.href
  const result: AuthDetails = await oidcClient.tryKeepExistingSessionAsync().then(async () => {
    if (href.includes(params.oidcConfig.redirect_uri)) {
      oidcClient.loginCallbackAsync().then(() => {
        window.location.href = `${base}/`
      })
      // loading...
    }

    let tokens = oidcClient.tokens

    if (!tokens) {
      return {
        login: async () => {
          params.onBeforeLogin?.()
          await oidcClient.loginAsync('/')
        }
      }
    }

    params.onAfterLogin?.()
    const userInfo = await oidcClient.userInfoAsync()

    client.interceptors.request.use(authRequestMiddleware)
    client.interceptors.response.use(authResponseMiddleware)

    return {
      logout: ({ callbackUrl }) =>
        oidcClient.logoutAsync(callbackUrl, params.logoutExtras),
      userInfo,
      profile: fromAxaUserInfo(userInfo),
      accessToken: tokens.accessToken // Only used in HTTP requests that cannot be handled with the global HTTP client instance from @hey-api/client-fetch
    }
  })
  return {
    auth: result
  }
}