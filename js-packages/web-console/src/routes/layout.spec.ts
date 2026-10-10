import { afterEach, describe, expect, it, vi } from 'vitest'

import type { load } from './+layout'

// The fake PostHog drops events sent before `init`, the same as the real SDK.
const m = vi.hoisted(() => {
  const state = { posthogReady: false, sent: [] as string[] }
  return {
    state,
    posthog: {
      init: () => {
        state.posthogReady = true
      },
      identify: () => {},
      reset: () => {},
      capture: (event: string) => {
        if (state.posthogReady) {
          state.sent.push(event)
        }
      }
    },
    getCachedConfigsForRender: vi.fn(),
    fetchConfigs: vi.fn()
  }
})

vi.mock('posthog-js', () => ({ default: m.posthog }))
vi.mock('$lib/services/conceptualHq', () => ({
  initConceptualHq: () => {},
  trackConceptualHq: () => {},
  trackConceptualHqSignup: () => {}
}))
vi.mock('$lib/services/productFruits', () => ({ initProductFruits: () => {} }))
vi.mock('$app/navigation', () => ({ goto: vi.fn(), invalidateAll: vi.fn() }))
vi.mock('$lib/compositions/auth', () => ({
  loadAuthConfig: async () => ({ oidc: { redirect_uri: 'http://console/auth/callback' } })
}))
vi.mock('@axa-fr/oidc-client', () => ({
  TokenAutomaticRenewMode: {},
  OidcLocation: class {},
  OidcClient: {
    getOrCreate: () => () => ({
      tryKeepExistingSessionAsync: async () => {},
      tokens: { idTokenPayload: { auth_time: 1000 }, accessToken: 'token' },
      userInfoAsync: async () => ({ sub: 'user-1', email: 'a@b.com' })
    })
  }
}))
vi.mock('$lib/compositions/configCache', () => ({
  clearConfigCaches: () => {},
  configChanged: () => true,
  fetchConfigs: m.fetchConfigs,
  getCachedConfigsForRender: m.getCachedConfigsForRender,
  getConfigFromCache: () => undefined,
  getSessionConfigFromCache: () => undefined
}))

const memoryStorage = () => {
  const store = new Map<string, string>()
  return {
    getItem: (k: string) => store.get(k) ?? null,
    setItem: (k: string, v: string) => store.set(k, v),
    removeItem: (k: string) => store.delete(k)
  }
}

// A new browser with no stored state. The layout, analytics and PostHog
// modules keep state in module variables, so each test imports new instances.
const pageLoad = async () => {
  m.state.posthogReady = false
  m.state.sent = []
  const localStorage = memoryStorage()
  vi.stubGlobal('localStorage', localStorage)
  vi.stubGlobal('window', {
    location: { href: 'http://console/' },
    localStorage,
    sessionStorage: memoryStorage()
  })
  vi.resetModules()
  return (await import('./+layout')).load({} as Parameters<typeof load>[0])
}

const config = (posthog: string) => ({ version: '1', edition: 'Open source', posthog })

afterEach(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe('root layout load', () => {
  it('sends signin on the cold path', async () => {
    m.getCachedConfigsForRender.mockReturnValue({})
    m.fetchConfigs.mockResolvedValue({ config: config('key'), sessionConfig: undefined })

    await pageLoad()

    expect(m.state.sent).toEqual(['signin'])
  })

  it('sends signin on the warm path when only the fresh config enables PostHog', async () => {
    vi.useFakeTimers()
    m.getCachedConfigsForRender.mockReturnValue({ config: config('') })
    m.fetchConfigs.mockResolvedValue({ config: config('key'), sessionConfig: undefined })

    await pageLoad()
    await vi.runAllTimersAsync()

    expect(m.state.sent).toEqual(['signin'])
  })
})
