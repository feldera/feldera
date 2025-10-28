import { getConfig, getConfigSession } from '$lib/services/pipelineManager'
import type { Configuration } from '$lib/services/manager'
import type { SessionInfo } from '$lib/services/manager'

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
