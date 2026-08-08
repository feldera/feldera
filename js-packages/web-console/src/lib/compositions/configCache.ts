import equal from 'fast-deep-equal'
import {
  errorCodeOf,
  isTenantRecheckPending,
  resetTenantRecheck
} from '$lib/compositions/tenantAccess'
import { getSelectedTenant, setSelectedTenant } from '$lib/services/auth'
import type { Configuration, SessionInfo } from '$lib/services/manager'
import { getConfig, getConfigSession } from '$lib/services/pipelineManager'

// Cache keys for localStorage
const CONFIG_CACHE_KEY = 'feldera_config_cache'
const SESSION_CONFIG_CACHE_KEY = 'feldera_session_config_cache'

// Cache helpers
export const getConfigFromCache = (): Configuration | undefined => {
  try {
    const cached = localStorage.getItem(CONFIG_CACHE_KEY)
    return cached ? JSON.parse(cached) : undefined
  } catch {
    return undefined
  }
}

export const getSessionConfigFromCache = (): SessionInfo | undefined => {
  try {
    const cached = localStorage.getItem(SESSION_CONFIG_CACHE_KEY)
    return cached ? JSON.parse(cached) : undefined
  } catch {
    return undefined
  }
}

export const setConfigCache = (config: Configuration) => {
  try {
    localStorage.setItem(CONFIG_CACHE_KEY, JSON.stringify(config))
  } catch (e) {
    console.warn('Failed to cache config:', e)
  }
}

export const setSessionConfigCache = (sessionConfig: SessionInfo | undefined) => {
  try {
    if (sessionConfig) {
      localStorage.setItem(SESSION_CONFIG_CACHE_KEY, JSON.stringify(sessionConfig))
    } else {
      localStorage.removeItem(SESSION_CONFIG_CACHE_KEY)
    }
  } catch (e) {
    console.warn('Failed to cache session config:', e)
  }
}

/**
 * The cached payloads a warm-cache render may use, or nothing when it must not.
 *
 * A pending tenant re-check means the server told this session it holds no
 * membership, so the cache still describes a tenant it no longer resolves.
 * Rendering from it would leave the app up while every request fails, so the
 * caller has to fetch and discover the unresolved state instead.
 */
export const getCachedConfigsForRender = (): {
  config?: Configuration
  sessionConfig?: SessionInfo
} => {
  if (isTenantRecheckPending()) {
    return {}
  }
  return { config: getConfigFromCache(), sessionConfig: getSessionConfigFromCache() }
}

const fetchConfigsOnce = async (): Promise<{
  config: Configuration | undefined
  sessionConfig: SessionInfo | undefined
}> => {
  const [config, sessionConfig] = await Promise.allSettled([getConfig(), getConfigSession()])

  if (
    sessionConfig.status === 'fulfilled' &&
    sessionConfig.value &&
    sessionConfig.value.tenant_id == null
  ) {
    // The login resolved no acting tenant (several memberships and no saved
    // selection, or none at all). /config/session is the only route that
    // answers in this state, so /config rejecting alongside is expected, not
    // an error. Drop the cached payloads: they describe a tenant this session
    // no longer resolves, and a warm-cache render from them would present the
    // app as if a tenant were still active.
    clearConfigCaches()
    return { config: undefined, sessionConfig: sessionConfig.value }
  }

  if (config.status === 'rejected') {
    throw config.reason
  }
  if (sessionConfig.status === 'rejected') {
    throw sessionConfig.reason
  }

  if (config.value) {
    setConfigCache(config.value)
    setSessionConfigCache(sessionConfig.value)
  }

  return { config: config.value, sessionConfig: sessionConfig.value }
}

// Error codes that mean the saved tenant selection stopped being valid, not
// that the request itself is broken: a member removed from the selected tenant
// gets `NotATenantMember`; an owner whose acted-as tenant was deleted gets
// `UnknownTenantName` (owners hold no memberships, so the server reports the
// missing tenant itself).
const INVALID_SELECTION_ERROR_CODES = ['NotATenantMember', 'UnknownTenantName']

/**
 * Fetch fresh config and session config, updating the localStorage cache.
 *
 * When the saved tenant selection stopped being valid (see
 * `INVALID_SELECTION_ERROR_CODES`), recover by dropping the selection and
 * retrying once headerless; the retry cannot loop because it runs with no
 * selection left to reject.
 */
export const fetchConfigs = async () => {
  try {
    const fetched = await fetchConfigsOnce()
    resetTenantRecheck()
    return fetched
  } catch (e) {
    const code = errorCodeOf(e)
    if (!code || !INVALID_SELECTION_ERROR_CODES.includes(code) || !getSelectedTenant()) {
      throw e
    }
    setSelectedTenant(undefined)
    const retried = await fetchConfigsOnce()
    resetTenantRecheck()
    return retried
  }
}

/**
 * Whether two `Configuration` payloads differ in a way that should motivate
 * re-invalidating downstream loads after a warm-cache reconcile. Fields that do
 * not affect rendered `page.data` are stripped before comparison so volatile
 * server-side values do not mask the short-circuit.
 *
 * Currently stripped: `license_validity.Exists.current`, a per-response
 * server timestamp consumed by server-time sync rather than by any view.
 * Extend this as new volatile-but-irrelevant fields appear.
 */
export const configChanged = (a: Configuration | undefined, b: Configuration | undefined) => {
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

export const clearConfigCaches = () => {
  try {
    localStorage.removeItem(CONFIG_CACHE_KEY)
    localStorage.removeItem(SESSION_CONFIG_CACHE_KEY)
  } catch (e) {
    console.warn('Failed to clear config caches:', e)
  }
}
