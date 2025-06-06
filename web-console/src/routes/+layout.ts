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
import type { Configuration, DisplaySchedule } from '$lib/services/manager'
import Dayjs from 'dayjs'
import duration from 'dayjs/plugin/duration'
import { initSystemMessages } from '$lib/compositions/initSystemMessages'
import { P, match } from 'ts-pattern'

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

type LayoutData = {
  auth: AuthDetails
  feldera: {
    version: string
    edition: string
    changelog?: string
    revision: string
    update?: {
      version: string
      url: string
    }
  }
}

const displayScheduleToDismissable = (schedule: DisplaySchedule) =>
  match(schedule)
    .with('Once', () => 'once' as const)
    .with('Session', () => 'session' as const)
    .with({ Every: P.select() }, ({ seconds }) => ({ milliseconds: seconds * 1000 }))
    .with('Always', () => 'never' as const)
    .exhaustive()

export const load = async ({ fetch, url }): Promise<LayoutData> => {
  if (!('window' in globalThis)) {
    return {
      auth: 'none',
      feldera: {
        version: '',
        edition: '',
        revision: ''
      }
    }
  }

  const authConfig = await loadAuthConfig()

  const auth = authConfig
    ? await axaOidcAuth({
        oidcConfig: toAxaOidcConfig(authConfig.oidc),
        logoutExtras: authConfig.logoutExtras,
        onBeforeLogin: () => window.sessionStorage.setItem('redirect_to', window.location.href),
        onAfterLogin: async () => {
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
    : 'none'

  if (typeof auth === 'object' && 'login' in auth) {
    return {
      auth,
      feldera: undefined!
    }
  }

  const config = await getConfig()

  if (typeof auth === 'object' && 'logout' in auth) {
    initPosthog(config).then(() => {
      if (auth.profile.email) {
        posthog.identify(auth.profile.email, {
          email: auth.profile.email,
          name: auth.profile.name,
          auth_id: auth.profile.id
        })
      }
    })
  }

  if (config.license_info && new Date(config.license_info.remind_starting_at) <= new Date()) {
    const expiresAt = Dayjs(config.license_info.expires_at)
    const time = expiresAt.isBefore()
      ? null
      : {
          in: `{toDaysHoursFromNow ${new Date(config.license_info.expires_at).valueOf()}}`,
          at: expiresAt.format('h A'),
          on: expiresAt.format('MMM D')
        }
    initSystemMessages.push({
      id: `expiring_license_${config.edition}`,
      dismissable: displayScheduleToDismissable(config.license_info.remind_schedule),
      text:
        (config.license_info.is_trial
          ? time
            ? `Your trial ends in ${time.in} at ${time.at} on ${time.on}`
            : 'Your trial has ended'
          : time
            ? `Your Feldera ${config.edition} license expires in ${time.in} at ${time.at} on ${time.on}`
            : `Your Feldera ${config.edition} license has expired`) +
        (config.license_info.description_html ? `. ${config.license_info.description_html}` : ''),
      action: config.license_info.extension_url
        ? {
            text: config.license_info.is_trial ? 'Upgrade Subscription' : 'Extend Your License',
            href: config.license_info.extension_url
          }
        : undefined
    })
  }
  if (config.update_info && !config.update_info.is_latest_version) {
    initSystemMessages.push({
      id: `version_available_${config.edition}_${config.update_info.latest_version}`,
      dismissable: displayScheduleToDismissable(config.update_info.remind_schedule),
      text: `New version ${config.update_info.latest_version} available`,
      action: {
        text: 'Update Now',
        href: config.update_info.instructions_url
      }
    })
  }

  return {
    auth,
    feldera: {
      version: config.version,
      edition: config.edition,
      update:
        config.update_info && !config.update_info.is_latest_version
          ? {
              version: config.update_info.latest_version,
              url: config.update_info.instructions_url
            }
          : undefined,
      changelog: config.changelog_url,
      revision: config.revision
    }
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
