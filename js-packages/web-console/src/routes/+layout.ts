import * as AxaOidc from '@axa-fr/oidc-client'
import Dayjs from 'dayjs'
import duration from 'dayjs/plugin/duration'
import equal from 'fast-deep-equal'
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
  errorResponseMiddleware,
  getSelectedTenant,
  setSelectedTenant,
  triggerOidcLogin
} from '$lib/services/auth'
import type { Configuration, SessionInfo } from '$lib/services/manager'
import { client } from '$lib/services/manager/client.gen'
import type { AuthDetails } from '$lib/types/auth'
import type { LayoutLoad } from './$types'

Dayjs.extend(duration)

const { OidcClient, OidcLocation } = AxaOidc

export const ssr = false
export const trailingSlash = 'always'

/**
 * Boot-up flow overview
 * ---------------------
 *
 * This file owns two responsibilities that together gate every page render:
 * OIDC auth initialization and Feldera backend-config loading. The goal is
 * for a returning user to see the UI on the *first* paint without a network
 * round-trip, while still converging to fresh server state in the background.
 *
 * Happy path (warm cache, no change on the server):
 *
 *   1. Module loads  →  `authInitPromise` starts once (OIDC bootstrap +
 *                       `/auth-config` fetch). Subsequent navigations await
 *                       the same settled promise.
 *   2. `load()` runs →  auth resolves  →  `getConfigFromCache()` returns
 *                       data from localStorage  →  layout returns synchronously.
 *                       The user sees the app immediately.
 *   3. `lazyUpdateConfig()` is scheduled exactly once per session, in the
 *      background. It fetches `/config` and `/config/session`, syncs server
 *      time, and compares the fresh payload against what was rendered.
 *   4. Configs match (the expected outcome on most visits)  →  the background
 *      fetch silently finishes. No `invalidateAll()`, no re-running of
 *      descendant `load` functions, no duplicate page-level fetches.
 *
 * Divergent paths:
 *
 *   - Cold cache (first ever visit, post-logout, post-clear): no cached
 *     config exists, so `load()` blocks on `fetchConfigs()` and renders from
 *     the fresh payload. No background update is scheduled because there is
 *     nothing to reconcile.
 *   - Config actually changed server-side: `configChanged()` / `equal()`
 *     detect the diff and `lazyUpdateConfig()` calls `invalidateAll()`. Every
 *     descendant `load` (including leaf `+page.ts` files) re-runs so views
 *     pick up the new values. The `lazyUpdateScheduled` guard ensures this
 *     does not re-enter `lazyUpdateConfig()`.
 *   - Unauthenticated: auth resolves to `{ login }`  →  empty layout is
 *     returned; `(authenticated)/+layout.ts` triggers `auth.login()` and the
 *     app redirects to the IdP.
 *
 * The warm-cache short-circuit is what keeps repeat visits from
 * double-fetching page-level resources, so be careful when adding new
 * branches that call `invalidateAll()` unconditionally.
 */

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
 * Delay before the warm-cache reconcile fires. Yields the origin's connection
 * pool and backend capacity to the page's critical path on slow links (e.g.
 * corporate VPN). The reconcile has no UI dependency on the warm-cache path,
 * so landing ~3s later only delays a rarely-needed `invalidateAll()` and re-initialization.
 */
const LAZY_UPDATE_DELAY_MS = 2000

/**
 * Hardcoded toggle for the warm-cache optimistic render. When `false`, every
 * `load()` blocks on `fetchConfigs()` (same shape as the cold-cache path) and
 * the `lazyUpdateConfig()` reconcile is skipped. The cache itself is still
 * written by `fetchConfigs()`, so flipping this back to `true` resumes
 * optimistic rendering without a stale-state warm-up step.
 */
const OPTIMISTIC_CONFIG_CACHE = true

/**
 * Whether two `Configuration` payloads differ in a way that should motivate
 * re-invalidating downstream loads in `lazyUpdateConfig`. Fields that do not
 * affect rendered `page.data` are stripped before comparison so volatile
 * server-side values do not mask the short-circuit.
 *
 * Currently stripped: `license_validity.Exists.current`, a per-response
 * server timestamp consumed by `syncServerTimeFromConfig` rather than by
 * any view. Extend this as new volatile-but-irrelevant fields appear.
 */
const configChanged = (a: Configuration | undefined, b: Configuration | undefined) => {
  const stripVolatile = (config: Configuration | undefined) => {
    if (!config) {
      return config
    }
    const license =
      config.license_validity && 'Exists' in config.license_validity
        ? config.license_validity.Exists
        : undefined
    if (!license) {
      return config
    }
    return {
      ...config,
      license_validity: { Exists: { ...license, current: undefined } }
    }
  }
  return !equal(stripVolatile(a), stripVolatile(b))
}

/**
 * Fetch fresh config, update the cache, and sync server time. If the server
 * config actually changed compared to what we rendered from cache, trigger
 * `invalidateAll()` so consumers of `page.data` pick up the new values.
 *
 * Server time is always synced — even when config is unchanged — because the
 * cached timestamp is stale by definition and we need a live one.
 *
 * The identity check avoids a needless `invalidateAll()` on every navigation
 * that hits a warm cache: `invalidateAll()` cascades into every descendant
 * `load` function (root layout, group layouts, leaf `+page.ts`), so on a
 * page like `/pipelines/[pipelineName]` a blind re-invalidation turns every
 * initial load into a duplicate fetch of the page's resources.
 *
 * The module-level `lazyUpdateScheduled` guard, combined with `initAuth`'s
 * own promise memoization, ensures the `invalidateAll()` re-run (when it
 * does fire) does not re-enter this path.
 */
const lazyUpdateConfig = async () => {
  // Snapshot cache BEFORE `fetchConfigs` overwrites it, so we can detect no-op
  // refreshes by comparing what we rendered against what the server returned.
  const prevConfig = getConfigFromCache()
  const prevSessionConfig = getSessionConfigFromCache()

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

  if (!configChanged(prevConfig, result.config) && equal(prevSessionConfig, result.sessionConfig)) {
    return
  }

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

// Register the error interceptor on the shared client up-front, independent of
// the OIDC handshake. It has to be in place before the first SDK call so that
// network failures and non-2xx responses both land in `errorResponseMiddleware`
// regardless of whether auth init has completed (or reached the success branch).
if ('window' in globalThis) {
  client.interceptors.error.use(errorResponseMiddleware)
}

const authInitPromise: Promise<AuthInitResult> = initAuth()

/**
 * Root layout load. Runs on every navigation, but the expensive parts are
 * memoized so descendant loads are not blocked past the first session.
 *
 * Flow:
 *
 *  1. **Auth init** — `authInitPromise` is built once at module load and
 *     awaited on every `load()` call. Its resolution is cached, so repeat
 *     navigations pay only the cost of `await` on an already-settled promise.
 *     Three outcomes:
 *       - `{ error }`              → render an error shell (`emptyLayoutData + error`).
 *       - `{ auth: { login } }`    → user is unauthenticated; short-circuit with
 *                                    the empty layout so `(authenticated)/+layout.ts`
 *                                    can trigger `auth.login()` and redirect.
 *       - `{ auth: { logout ...}}` → proceed to config loading.
 *
 *  2. **Config loading** — warm localStorage cache is the common path:
 *       - **Warm cache (most navigations):** return cached `feldera` data
 *         synchronously, run idempotent side effects (tenant selection,
 *         posthog init, system messages), and kick off exactly one
 *         background `lazyUpdateConfig()` per session (guarded by
 *         `lazyUpdateScheduled`). The UI renders immediately with cached
 *         values; the background fetch only triggers an `invalidateAll()`
 *         if the server's config differs from what we rendered, so repeat
 *         visits do not double-fetch page-level resources.
 *       - **Cold cache (first visit / after logout / after manual clear):**
 *         block on `fetchConfigs()`, sync server time, and render with the
 *         fresh data. No background update is scheduled because the fetched
 *         values are already canonical.
 *
 * Server time is intentionally NOT restored from cache: the cached
 * `license.current` timestamp is stale and applying it would corrupt the
 * offset. Until fresh data arrives we let `Date.now()` stand in.
 */
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

  const cachedConfig = OPTIMISTIC_CONFIG_CACHE ? getConfigFromCache() : undefined
  const cachedSessionConfig = OPTIMISTIC_CONFIG_CACHE ? getSessionConfigFromCache() : undefined

  if (cachedConfig) {
    initializeConfigDependencies(auth, cachedConfig)
    if (!lazyUpdateScheduled) {
      lazyUpdateScheduled = true
      setTimeout(() => {
        lazyUpdateConfig()
      }, LAZY_UPDATE_DELAY_MS)
    }
    return buildLayoutData(auth, cachedConfig, cachedSessionConfig)
  }

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
            login: triggerOidcLogin
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

        triggerOidcLogin()
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
