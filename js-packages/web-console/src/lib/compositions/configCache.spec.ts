// Unit tests for the warm-cache machinery used by the root `+layout.ts`
// boot-up flow: cached reads (hit/miss/corrupt), writes, the lazy-load
// `fetchConfigs` orchestrator (success + failure), and `configChanged`
// reconcile semantics that gate `invalidateAll()` after a warm-cache render.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Configuration, SessionInfo } from '$lib/services/manager'

vi.mock('$lib/services/pipelineManager', () => ({
  getConfig: vi.fn(),
  getConfigSession: vi.fn()
}))

import { getConfig, getConfigSession } from '$lib/services/pipelineManager'
import {
  clearConfigCaches,
  configChanged,
  fetchConfigs,
  getConfigFromCache,
  getSessionConfigFromCache,
  setConfigCache,
  setSessionConfigCache
} from './configCache'

const CONFIG_KEY = 'feldera_config_cache'
const SESSION_KEY = 'feldera_session_config_cache'

const mockedGetConfig = getConfig as unknown as ReturnType<typeof vi.fn>
const mockedGetConfigSession = getConfigSession as unknown as ReturnType<typeof vi.fn>

// Minimal in-memory localStorage polyfill: vitest's `node` environment does
// not provide one, and we want to exercise the real `setItem`/`getItem` round
// trip rather than mocking each call.
class MemoryStorage {
  private store = new Map<string, string>()
  getItem = (k: string) => (this.store.has(k) ? (this.store.get(k) as string) : null)
  setItem = (k: string, v: string) => {
    this.store.set(k, String(v))
  }
  removeItem = (k: string) => {
    this.store.delete(k)
  }
  clear = () => {
    this.store.clear()
  }
}

// Minimal shapes — these tests only inspect equality and a handful of fields,
// so we cast through `unknown` instead of populating every required SDK field.
const baseConfig = {
  build_info: { build_cpu: 'x86', build_os: 'linux', build_timestamp: '2026-01-01' },
  build_source: 'source',
  changelog_url: 'https://example/cl',
  edition: 'Open source',
  revision: 'abc123',
  runtime_revision: 'rt123',
  telemetry: '',
  version: '0.0.0'
} as unknown as Configuration

const sessionConfig = {
  tenant_id: 't-1',
  tenant_name: 'Tenant'
} as unknown as SessionInfo

beforeEach(() => {
  vi.stubGlobal('localStorage', new MemoryStorage())
  mockedGetConfig.mockReset()
  mockedGetConfigSession.mockReset()
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('configCache primitives', () => {
  it('returns undefined on a cache miss', () => {
    expect(getConfigFromCache()).toBeUndefined()
    expect(getSessionConfigFromCache()).toBeUndefined()
  })

  it('round-trips a config hit', () => {
    setConfigCache(baseConfig)
    expect(getConfigFromCache()).toEqual(baseConfig)
  })

  it('round-trips a session config hit', () => {
    setSessionConfigCache(sessionConfig)
    expect(getSessionConfigFromCache()).toEqual(sessionConfig)
  })

  it('removes the session entry when called with undefined', () => {
    setSessionConfigCache(sessionConfig)
    setSessionConfigCache(undefined)
    expect(localStorage.getItem(SESSION_KEY)).toBeNull()
  })

  it('treats malformed JSON as a miss instead of throwing', () => {
    // Simulate a partial write or schema drift by stuffing the slot with a
    // non-JSON string. Callers must never see the SyntaxError.
    localStorage.setItem(CONFIG_KEY, '{not json')
    localStorage.setItem(SESSION_KEY, '{also not json')
    expect(getConfigFromCache()).toBeUndefined()
    expect(getSessionConfigFromCache()).toBeUndefined()
  })

  it('clearConfigCaches removes both keys', () => {
    setConfigCache(baseConfig)
    setSessionConfigCache(sessionConfig)
    clearConfigCaches()
    expect(localStorage.getItem(CONFIG_KEY)).toBeNull()
    expect(localStorage.getItem(SESSION_KEY)).toBeNull()
  })
})

describe('fetchConfigs', () => {
  it('caches both payloads on a successful fetch', async () => {
    mockedGetConfig.mockResolvedValueOnce(baseConfig)
    mockedGetConfigSession.mockResolvedValueOnce(sessionConfig)

    const result = await fetchConfigs()

    expect(result.config).toEqual(baseConfig)
    expect(result.sessionConfig).toEqual(sessionConfig)
    expect(getConfigFromCache()).toEqual(baseConfig)
    expect(getSessionConfigFromCache()).toEqual(sessionConfig)
  })

  it('does not cache when the backend returns no config', async () => {
    setConfigCache(baseConfig) // pre-existing cache from a prior session
    mockedGetConfig.mockResolvedValueOnce(undefined)
    mockedGetConfigSession.mockResolvedValueOnce(undefined)

    const result = await fetchConfigs()

    expect(result.config).toBeUndefined()
    // The previous cache entry is preserved — we don't blow away a known-good
    // value just because this round-trip returned nothing.
    expect(getConfigFromCache()).toEqual(baseConfig)
  })

  it('propagates the SDK error so the caller can decide how to recover (lazy-load failure path)', async () => {
    const networkError = new Error('network down')
    mockedGetConfig.mockRejectedValueOnce(networkError)
    mockedGetConfigSession.mockResolvedValueOnce(sessionConfig)

    await expect(fetchConfigs()).rejects.toBe(networkError)
    // The cache remains empty — we never half-write a partial result.
    expect(getConfigFromCache()).toBeUndefined()
  })

  it('clears a stale session entry when the new payload omits it', async () => {
    setSessionConfigCache(sessionConfig)
    mockedGetConfig.mockResolvedValueOnce(baseConfig)
    mockedGetConfigSession.mockResolvedValueOnce(undefined)

    await fetchConfigs()

    expect(getSessionConfigFromCache()).toBeUndefined()
  })
})

describe('configChanged (cache reconcile)', () => {
  // The `+layout.ts` warm-cache reconcile uses this predicate to decide
  // whether to call `invalidateAll()` after the lazy refresh. False positives
  // here turn every warm visit into a duplicate descendant load, so the
  // identity-on-equal contract is load-bearing.

  const withLicense = (current: string): Configuration =>
    ({
      ...baseConfig,
      license_validity: {
        Exists: {
          current,
          description_html: '',
          is_trial: false,
          remind_schedule: 'never',
          valid_until: '2099-01-01'
        }
      }
    }) as unknown as Configuration

  it('returns false on the cache-hit reconcile (server returned the same payload)', () => {
    expect(configChanged(baseConfig, { ...baseConfig })).toBe(false)
  })

  it('returns false when only the volatile `license.current` timestamp differs', () => {
    // The server stamps every /config response with a fresh `current` time —
    // without volatile-stripping this would force `invalidateAll()` on every
    // warm visit and undo the whole point of the optimistic cache.
    const a = withLicense('2026-01-01T00:00:00Z')
    const b = withLicense('2026-01-01T00:00:05Z')
    expect(configChanged(a, b)).toBe(false)
  })

  it('returns true when a meaningful field changes (cache invalidation path)', () => {
    expect(configChanged(baseConfig, { ...baseConfig, version: '0.0.1' })).toBe(true)
    expect(configChanged(baseConfig, { ...baseConfig, edition: 'Enterprise' })).toBe(true)
  })

  it('returns true when one side is undefined and the other is not', () => {
    expect(configChanged(undefined, baseConfig)).toBe(true)
    expect(configChanged(baseConfig, undefined)).toBe(true)
  })

  it('returns false when both sides are undefined', () => {
    expect(configChanged(undefined, undefined)).toBe(false)
  })

  it('returns true when a non-volatile license field changes', () => {
    const a = withLicense('2026-01-01T00:00:00Z')
    const b: Configuration = {
      ...a,
      license_validity: { DoesNotExistOrNotConfirmed: 'expired' }
    }
    expect(configChanged(a, b)).toBe(true)
  })
})
