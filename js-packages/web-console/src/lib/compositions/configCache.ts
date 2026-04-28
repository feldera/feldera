import equal from 'fast-deep-equal'
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

// Fetch fresh config and session config
export const fetchConfigs = async () => {
  const [config, sessionConfig] = await Promise.all([getConfig(), getConfigSession()])

  if (config) {
    setConfigCache(config)
    setSessionConfigCache(sessionConfig)
  }

  return { config, sessionConfig }
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
