import '$lib/compositions/setupHttpClient'
import { loadAuthConfig } from '$lib/compositions/auth'
import * as AxaOidc from '@axa-fr/oidc-client'
import { fromAxaUserInfo, toAxaOidcConfig } from '$lib/compositions/@axa-fr/auth'
import { client } from '@hey-api/client-fetch'
import { base } from '$app/paths'
import {
  authRequestMiddleware,
  authResponseMiddleware,
  getSelectedTenant,
  setSelectedTenant
} from '$lib/services/auth'
import type { AuthDetails } from '$lib/types/auth'
import { goto } from '$app/navigation'
import posthog from 'posthog-js'
import { getConfig, getConfigSession } from '$lib/services/pipelineManager'
import type { Configuration } from '$lib/services/manager'
import Dayjs from 'dayjs'
import duration from 'dayjs/plugin/duration'
import { initSystemMessages } from '$lib/compositions/initSystemMessages'
import { newDate, setCurrentTime } from '$lib/compositions/serverTime'
import { displayScheduleToDismissable, getLicenseMessage } from '$lib/functions/license'
import { jwtDecode } from 'jwt-decode'

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

export type LayoutData = {
  auth: AuthDetails
  feldera:
    | {
        version: string
        edition: string
        changelog?: string
        revision: string
        update?: {
          version: string
          url: string
        }
        tenantId: string
        tenantName: string
        /**
         * Only available if authenticated and using multi-tenant authorization
         */
        authorizedTenants?: string[]
        unstableFeatures: string[]
        config: Configuration
      }
    | undefined
  error?: Error
}

const emptyLayoutData: LayoutData = {
  auth: 'none',
  feldera: undefined
}

const processTenants = (auth: AuthDetails) => {
  let authorizedTenants: string[] | undefined = undefined

  if (typeof auth === 'object' && 'logout' in auth) {
    const tenantsString = auth.accessToken
      ? jwtDecode<{ tenants?: string[] | string }>(auth.accessToken).tenants
      : undefined
    authorizedTenants = tenantsString
      ? Array.isArray(tenantsString)
        ? tenantsString
        : tenantsString.split(',').map((t) => t.trim())
      : undefined
    if (authorizedTenants) {
      const savedTenant = getSelectedTenant()
      console.log('authorizedTenants', authorizedTenants, savedTenant)
      if (!savedTenant || !authorizedTenants.includes(savedTenant)) {
        setSelectedTenant(authorizedTenants[0])
      }
    }
  }

  return authorizedTenants
}

export const load = async ({ fetch, url }): Promise<LayoutData> => {
  if (!('window' in globalThis)) {
    return emptyLayoutData
  }

  const authConfig = await loadAuthConfig()

  const auth = authConfig
    ? await axaOidcAuth({
        oidcConfig: { ...toAxaOidcConfig(authConfig.oidc) },
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
      ...emptyLayoutData,
      auth
    }
  }

  const authorizedTenants = processTenants(auth)

  let config: Configuration | undefined = undefined

  try {
    config = await getConfig()
  } catch (e: any) {
    if (e.cause.response.status === 401 || e.cause.response.status === 403) {
      return {
        ...emptyLayoutData,
        auth,
        error: e
      }
    }
  }

  if (!config) {
    console.error('Failed to load configuration')
    return emptyLayoutData
  }

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

  // Fetch session config for tenant information (only available if authenticated)
  let sessionConfig = undefined
  try {
    sessionConfig = await getConfigSession()
  } catch (e: any) {
    // Session config might not be available if not authenticated, which is fine
    console.warn('Failed to load session configuration:', e)
  }

  {
    const license =
      config.license_validity && 'Exists' in config.license_validity
        ? config.license_validity.Exists
        : undefined

    if (license) {
      setCurrentTime(license.current)
    }
  }
  {
    const message = getLicenseMessage(config, newDate())
    if (message) {
      initSystemMessages.push(message)
    }
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
      revision: config.revision,
      tenantId: sessionConfig?.tenant_id || '',
      tenantName: sessionConfig?.tenant_name || '',
      authorizedTenants,
      unstableFeatures: config.unstable_features?.split(',').map((f) => f.trim()) || [],
      config
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
  const oidcClient = OidcClient.getOrCreate(
    () => fetch,
    new OidcLocation()
  )({ ...params.oidcConfig, extras: { audience: 'feldera-api' } })
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
