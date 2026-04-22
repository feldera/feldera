import * as AxaOidc from '@axa-fr/oidc-client'
import Dayjs from 'dayjs'
import duration from 'dayjs/plugin/duration'
import { jwtDecode } from 'jwt-decode'
import posthog from 'posthog-js'
import { goto, invalidateAll } from '$app/navigation'
import { fromAxaUserInfo, toAxaOidcConfig } from '$lib/compositions/@axa-fr/auth'
import { loadAuthConfig } from '$lib/compositions/auth'
import {
  clearConfigCaches,
  fetchConfigs,
  getConfigFromCache,
  getSessionConfigFromCache
} from '$lib/compositions/configCache'
import { initSystemMessages, type SystemMessage } from '$lib/compositions/initSystemMessages'
import { newDate, setCurrentTime } from '$lib/compositions/serverTime'
import { displayScheduleToDismissable, getLicenseMessage } from '$lib/functions/license'
import { resolve } from '$lib/functions/svelte'
import {
  authRequestMiddleware,
  authResponseMiddleware,
  getSelectedTenant,
  setSelectedTenant
} from '$lib/services/auth'
import type { Configuration, SessionInfo } from '$lib/services/manager'
import { client } from '$lib/services/manager/client.gen'
import type { AuthDetails } from '$lib/types/auth'
import type { LayoutLoad } from './$types'

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

const computeAuthorizedTenants = (auth: AuthDetails): string[] | undefined => {
  if (typeof auth !== 'object' || !('logout' in auth)) {
    return undefined
  }
  const tenantsString = auth.accessToken
    ? jwtDecode<{ tenants?: string[] | string }>(auth.accessToken).tenants
    : undefined
  return tenantsString
    ? Array.isArray(tenantsString)
      ? tenantsString
      : tenantsString.split(',').map((t) => t.trim())
    : undefined
}

const applyTenantSelection = (authorizedTenants: string[] | undefined) => {
  if (authorizedTenants) {
    const savedTenant = getSelectedTenant()
    if (!savedTenant || !authorizedTenants.includes(savedTenant)) {
      setSelectedTenant(authorizedTenants[0])
    }
  } else {
    setSelectedTenant(undefined)
  }
}

const pushSystemMessageOnce = (message: SystemMessage) => {
  if (!initSystemMessages.some((m) => m.id === message.id)) {
    initSystemMessages.push(message)
  }
}

/**
 * Sync the client/server time offset from a fresh license payload. Called only
 * after we receive fresh data from the server — cached `license.current` is a
 * stale timestamp and applying it would corrupt the offset, so we leave
 * `offsetMs = 0` (i.e. treat `Date.now()` as the current server time) until a
 * real response arrives.
 */
const syncServerTimeFromConfig = (config: Configuration) => {
  const license =
    config.license_validity && 'Exists' in config.license_validity
      ? config.license_validity.Exists
      : undefined
  if (license) {
    setCurrentTime(license.current)
  }
}

/**
 * Resolves once the SvelteKit client router has initialized (first
 * `afterNavigate` fires from the root layout). Guards `invalidateAll()` in
 * `lazyUpdateConfig`, which would otherwise throw when the background fetch
 * resolves before the initial navigation completes.
 */
let _resolveRouterReady: () => void
const routerReady = new Promise<void>((resolve) => {
  _resolveRouterReady = resolve
})
export const _markRouterReady = () => _resolveRouterReady()

let lazyUpdateScheduled = false

/**
 * Fetch fresh config, update the cache, sync server time, then trigger a load
 * re-run so consumers of `page.data` see the refreshed config. Fired from the
 * cache-hit path so a stale cache doesn't persist forever. The module-level
 * guard ensures the `invalidateAll()` re-run doesn't re-enter this path.
 */
const lazyUpdateConfig = async () => {
  let result
  try {
    result = await fetchConfigs()
  } catch (e) {
    console.warn('Background config refresh failed:', e)
    return
  }
  if (!result.config) {
    return
  }
  syncServerTimeFromConfig(result.config)
  await routerReady
  await invalidateAll()
}

type AuthInitResult = { auth: AuthDetails } | { error: Error }

/**
 * One-shot OIDC initialization. Kicks off when this module is first imported
 * (in the browser — `ssr = false` means module-level code only runs client-side
 * during route setup). `load()` awaits this promise on every run, so repeated
 * load invocations (e.g. from `invalidateAll()`) reuse the same result instead
 * of refetching `/auth-config` and re-running the OIDC handshake.
 */
const initAuth = async (): Promise<AuthInitResult> => {
  if (!('window' in globalThis)) {
    return { auth: 'none' }
  }
  const authConfig = await loadAuthConfig()
  if (!authConfig) {
    return { auth: 'none' }
  }
  return axaOidcAuth({
    oidcConfig: { ...toAxaOidcConfig(authConfig.oidc) },
    logoutExtras: authConfig.logoutExtras,
    onBeforeLogin: () => window.sessionStorage.setItem('redirect_to', window.location.href),
    onAfterLogin: async () => {
      const redirectTo = window.sessionStorage.getItem('redirect_to')
      if (!redirectTo) {
        return
      }
      window.sessionStorage.removeItem('redirect_to')
      goto(redirectTo)
    },
    onBeforeLogout() {
      // Session-scoped data must not survive a logout/login cycle, since a different
      // user may sign in on the same browser.
      clearConfigCaches()
      posthog.reset()
    }
  })
}

const authInitPromise: Promise<AuthInitResult> = initAuth()

export const load: LayoutLoad = async (): Promise<LayoutData> => {
  if (!('window' in globalThis)) {
    return emptyLayoutData
  }

  const authState = await authInitPromise

  if ('error' in authState) {
    return {
      ...emptyLayoutData,
      error: authState.error
    }
  }

  const auth = authState.auth

  if (typeof auth === 'object' && 'login' in auth) {
    return {
      ...emptyLayoutData,
      auth
    }
  }

  // Get cached config if available
  const cachedConfig = getConfigFromCache()
  const cachedSessionConfig = getSessionConfigFromCache()

  if (cachedConfig) {
    // Return cached layout data synchronously. Run the non-server-time side
    // effects immediately so tenant selection / posthog / system messages are
    // consistent with what the user last saw. Server time is intentionally not
    // restored from cache — the cached timestamp is stale and would corrupt
    // the offset; instead we let `Date.now()` stand in until `lazyUpdateConfig`
    // brings a fresh value.
    initializeConfigDependencies(auth, cachedConfig)
    if (!lazyUpdateScheduled) {
      lazyUpdateScheduled = true
      void lazyUpdateConfig()
    }
    return buildLayoutData(auth, cachedConfig, cachedSessionConfig)
  }

  // If we have no cached config, wait for the first load
  let result
  try {
    result = await fetchConfigs()
  } catch (e: any) {
    if (e.cause?.response?.status === 401 || e.cause?.response?.status === 403) {
      return {
        ...emptyLayoutData,
        auth,
        error: e
      }
    }
    console.error('Failed to load configuration')
    return emptyLayoutData
  }

  if (!result.config) {
    console.error('Failed to load configuration')
    return emptyLayoutData
  }

  syncServerTimeFromConfig(result.config)
  initializeConfigDependencies(auth, result.config)

  return buildLayoutData(auth, result.config, result.sessionConfig)
}

function buildFelderaData(
  auth: AuthDetails,
  config: Configuration,
  sessionConfig: SessionInfo | undefined
) {
  return {
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
    authorizedTenants: computeAuthorizedTenants(auth),
    unstableFeatures: config.unstable_features?.split(',').map((f: string) => f.trim()) || [],
    config
  }
}

function buildLayoutData(
  auth: AuthDetails,
  config: Configuration,
  sessionConfig: SessionInfo | undefined
): LayoutData {
  return {
    auth,
    feldera: buildFelderaData(auth, config, sessionConfig)
  }
}

/**
 * Run side effects derived from `config` that are independent of server-time
 * sync. Safe to call with cached config and safe to re-run: all pushes are
 * deduped by id, tenant selection is idempotent for a stable auth, and
 * `posthog.init` dedupes on its key.
 * It is expected to be idempotent - calling it the second time on lazy update
 * when config hasn't changed should not break anything.
 *
 * Does NOT call `setCurrentTime` — that belongs to `syncServerTimeFromConfig`,
 * which only runs against freshly-fetched data.
 */
function initializeConfigDependencies(auth: AuthDetails, config: Configuration) {
  applyTenantSelection(computeAuthorizedTenants(auth))

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

  {
    const message = getLicenseMessage(config, newDate())
    if (message) {
      pushSystemMessageOnce(message)
    }
  }

  if (config.update_info && !config.update_info.is_latest_version) {
    pushSystemMessageOnce({
      id: `version_available_${config.edition}_${config.update_info.latest_version}`,
      dismissable: displayScheduleToDismissable(config.update_info.remind_schedule),
      text: `New version ${config.update_info.latest_version} available`,
      action: {
        text: 'Update Now',
        href: config.update_info.instructions_url
      }
    })
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
  const result: { auth: AuthDetails } | { error: Error } = await oidcClient
    .tryKeepExistingSessionAsync()
    .then(async () => {
      if (href.includes(params.oidcConfig.redirect_uri)) {
        oidcClient.loginCallbackAsync().then(() => {
          window.location.href = resolve(`/`)
        })
        // loading...
      }

      const tokens = oidcClient.tokens

      if (!tokens) {
        return {
          auth: {
            login: async () => {
              params.onBeforeLogin?.()
              await oidcClient.loginAsync('/')
            }
          }
        }
      }

      const userInfoPromise = oidcClient.userInfoAsync()
      params.onAfterLogin?.(tokens.idTokenPayload, userInfoPromise)
      const userInfo = await userInfoPromise

      if (!userInfo) {
        console.error(
          'Failed to retrieve user info - authentication has probably changed on the server. Automatically attempting to re-authenticate...'
        )

        oidcClient.loginAsync('/')
        return {
          error: new Error(
            'Failed to retrieve user info - authentication has probably changed on the server. Automatically attempting to re-authenticate...'
          )
        }
      }

      client.interceptors.request.use(authRequestMiddleware)
      client.interceptors.response.use(authResponseMiddleware)

      return {
        auth: {
          logout: ({ callbackUrl }) => {
            params.onBeforeLogout?.()
            return oidcClient.logoutAsync(callbackUrl, params.logoutExtras)
          },
          userInfo,
          profile: fromAxaUserInfo(userInfo),
          accessToken: tokens.accessToken // Only used in HTTP requests that cannot be handled with the global HTTP client instance from @hey-api/client-fetch
        }
      }
    })
  return result
}
