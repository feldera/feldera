import '$lib/compositions/setupHttpClient'
import { loadAuthConfig, type UserProfile } from '$lib/compositions/auth'
import * as AxaOidc from '@axa-fr/oidc-client'
import type { OidcUserInfo } from '@axa-fr/oidc-client'
import { fromAxaUserInfo, toAxaOidcConfig } from '$lib/compositions/@axa-fr/auth'
import { client } from '@hey-api/client-fetch'
import { base } from '$app/paths'
const { OidcClient, OidcLocation } = AxaOidc

export const ssr = false
export const trailingSlash = 'always'

let accessToken: string | undefined
const authRequestMiddleware = (request: Request) => {
  if (accessToken) {
    request.headers.set('Authorization', `Bearer ${accessToken}`)
  }
  return request
}

const authResponseMiddleware = async (response: Response, request: Request) => {
  if (response.status === 401) {
    console.log('trying to relogin')
    const client = OidcClient.get()
    await client.renewTokensAsync()
    accessToken = client.tokens.accessToken
    console.log('succ!')
    return fetch(request)
  }
  return response
}

type AuthDetails =
  | 'none'
  | {
      login: () => Promise<void>
    }
  | {
      logout: (params: { callbackUrl: string }) => Promise<void>
      userInfo: OidcUserInfo
      profile: UserProfile
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
  const oidcClient = OidcClient.getOrCreate(
    () => globalThis.fetch,
    new OidcLocation()
  )(axaOidcConfig)
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

    accessToken = tokens.accessToken
    client.interceptors.request.use(authRequestMiddleware)
    client.interceptors.response.use(authResponseMiddleware)
    return {
      logout: ({ callbackUrl }) => oidcClient.logoutAsync(callbackUrl, authConfig.logoutExtras),
      userInfo,
      profile: fromAxaUserInfo(userInfo)
    }
  })
  return {
    auth: result
  }
}
