import '$lib/compositions/setupHttpClient'
import { loadAuthConfig } from '$lib/compositions/auth'
import * as AxaOidc from '@axa-fr/oidc-client'
import { fromAxaUserInfo, toAxaOidcConfig } from '$lib/compositions/@axa-fr/auth'
import { client } from '@hey-api/client-fetch'
import { base } from '$app/paths'
import { authRequestMiddleware, authResponseMiddleware } from '$lib/services/auth'
import type { AuthDetails } from '$lib/types/auth'
import { goto } from '$app/navigation'
import posthog from 'posthog-js'
import { getConfig } from '$lib/services/pipelineManager'
import type { Configuration } from '$lib/services/manager'
import Dayjs from 'dayjs'
import duration from 'dayjs/plugin/duration'

Dayjs.extend(duration)

const { OidcClient, OidcLocation } = AxaOidc

export const ssr = false
export const trailingSlash = 'always'

const initPosthog = async (config: Configuration) => {
  if (!config.telemetry) {
    return
  }
  posthog.init(config.telemetry, {
    api_host: 'https://us.i.posthog.com',
    person_profiles: 'identified_only',
    capture_pageview: false,
    capture_pageleave: false
  })
}

export const load = async ({
  fetch,
  url
}): Promise<{ auth: AuthDetails; felderaVersion: string }> => {
  if (!('window' in globalThis)) {
    return {
      auth: 'none',
      felderaVersion: ''
    }
  } else {
  }

  const [config, authConfig] = await Promise.all([getConfig(), loadAuthConfig()])
  if (!authConfig) {
    return {
      auth: 'none',
      felderaVersion: config.version
    }
  }
  const axaOidcConfig = toAxaOidcConfig(authConfig.oidc)
  const auth = await axaOidcAuth({
    oidcConfig: axaOidcConfig,
    logoutExtras: authConfig.logoutExtras,
    onBeforeLogin: () => window.sessionStorage.setItem('redirect_to', window.location.href),
    onAfterLogin: async (idTokenPayload) => {
      {
        initPosthog(config).then(() => {
          if (idTokenPayload?.email) {
            posthog.identify(idTokenPayload.email, {
              email: idTokenPayload.email,
              name: idTokenPayload.name
            })
          }
        })
      }
      {
        const redirectTo = window.sessionStorage.getItem('redirect_to')
        if (!redirectTo) {
          return
        }
        window.sessionStorage.removeItem('redirect_to')
        goto(redirectTo)
      }
    },
    onBeforeLogout() {
      posthog.reset()
    }
  })
  return {
    auth,
    felderaVersion: config.version
  }
}

const axaOidcAuth = async (params: {
  oidcConfig: AxaOidc.OidcConfiguration
  logoutExtras?: AxaOidc.StringMap
  onBeforeLogin?: () => void
  onAfterLogin?: (idTokenPayload: any, userInfo: Promise<AxaOidc.OidcUserInfo>) => void
  onBeforeLogout?: () => void
}) => {
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

    const userInfoPromise = oidcClient.userInfoAsync()
    params.onAfterLogin?.(tokens.idTokenPayload, userInfoPromise)
    const userInfo = await userInfoPromise

    client.interceptors.request.use(authRequestMiddleware)
    client.interceptors.response.use(authResponseMiddleware)

    return {
      logout: ({ callbackUrl }) => {
        params.onBeforeLogout?.()
        return oidcClient.logoutAsync(callbackUrl, params.logoutExtras)
      },
      userInfo,
      profile: fromAxaUserInfo(userInfo),
      accessToken: tokens.accessToken // Only used in HTTP requests that cannot be handled with the global HTTP client instance from @hey-api/client-fetch
    }
  })
  return result
}
